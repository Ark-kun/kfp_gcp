import json
import logging
import subprocess


def mirror_and_replace_container_images(pipeline_job: dict, mirror_prefix: str, project_id: str = None)-> dict:
    container_images = _get_all_used_images(pipeline_job)
    replacement_images = {
        image: mirror_prefix + image
        for image in container_images
        if not image.startswith('gcr.io/')
    }

    # Mirror the images
    logging.debug('Mirroring container images: Checking for existing mirrors.')
    images_to_mirror = {}
    for image, replacement in replacement_images.items():
        if not _inspect_google_container_registry_image(replacement):
            images_to_mirror[image] = replacement
    
    if images_to_mirror:
        _mirror_images_using_gcloud_build(images_to_mirror, project_id)
    
    patched_pipeline_job = _replace_used_images(pipeline_job, replacement_images)
    return patched_pipeline_job


def _get_all_used_images(pipeline_job_dict: dict) -> set:
    images = set()
    steps = pipeline_job_dict['spec']['steps']
    for step in steps.values():
        image = step['task']['container']['image']
        images.add(image)
    return images


def _replace_used_images(pipeline_job_dict: dict, replacement_map: dict) -> set:
    import copy
    pipeline_job_dict = copy.deepcopy(pipeline_job_dict)
    steps = pipeline_job_dict['spec']['steps']
    for step in steps.values():
        image = step['task']['container']['image']
        step['task']['container']['image'] = replacement_map.get(image, image)
    return pipeline_job_dict


def _inspect_google_container_registry_image(image: str, project_id: str = None) -> dict:
    command_line = ['gcloud', 'container', 'images', 'describe', image, '--format', 'json']
    if project_id:
        command_line.extend(['--project', project_id])

    process_run = subprocess.run(
        command_line,
        capture_output=True,
    )
    if process_run.returncode != 0:
        return None
    return json.loads(process_run.stdout)


def _prepare_cloudbuild_config_that_mirrors_images(image_mirrors: dict) -> dict:
    build_steps = []
    build_images = []
    build_config = dict(
        steps=build_steps,
        images=build_images,
    )
    for src_image, dst_image in image_mirrors.items():
        build_images.append(dst_image)
        build_step = dict(
            name='gcr.io/cloud-builders/docker',
            waitFor=['-'],
            entrypoint='bash',
            args=[
                '-exc',
                '\n'.join([
                    'docker pull --quiet ' + src_image,
                    'docker tag ' + src_image + ' ' + dst_image,
                    #'docker push ' + dst_image,
                ]),
            ],
        )
        build_steps.append(build_step)

        return build_config


def _mirror_images_using_gcloud_build(image_mirrors: dict, project_id: str = None) -> None:
    logging.info('Mirroring container images: ' + str(image_mirrors))
    build_config = _prepare_cloudbuild_config_that_mirrors_images(image_mirrors)
    import tempfile
    #with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml') as cloudbuild_file:
    with tempfile.NamedTemporaryFile(mode='w', delete=False) as cloudbuild_file:
        json.dump(build_config, cloudbuild_file.file, indent=True)
        cloudbuild_file.file.close()
        command_line = ['gcloud', 'builds', 'submit', '--config', cloudbuild_file.name, '--no-source', '--quiet']
        if project_id:
            command_line.extend(['--project', project_id])
        process_run = subprocess.run(
            command_line,
            capture_output=True,
        )
        if process_run.returncode != 0:
            raise RuntimeError('Image mirroring failed: STDERR=' + process_run.stderr.decode('utf-8'))
