import datetime
import json
import logging
import pathlib
import re
import time
from typing import Callable, Dict, List

from kfp import components
from kfp.components import structures
from kfp.components import _components

from . import _pipeline_jobs_api
from . import _image_mirroring


def _generate_command_line(
    user_command_line: List[str],
    input_path_uris: Dict[str, str],
    output_path_uris: Dict[str, str],
) -> List[str]:
    if not input_path_uris and not output_path_uris:
        return user_command_line

    code_lines = '''
# Installing wget if it's missing
if ! which wget && ! which curl; then
  if apt-get update -qq -o=Dpkg::Use-Pty=0; then
    apt-get install wget -qq -o=Dpkg::Use-Pty=0
  fi
fi

gcs_copy_url=https://github.com/Ark-kun/gcs_copy_go/releases/download/v0.2/gcs_copy-linux-amd64
bin_dir=/tmp/kfp_bin/
gcs_copy_path="${bin_dir}/gcs_copy"
mkdir -p "$bin_dir"
wget "$gcs_copy_url" --output-document "$gcs_copy_path" --no-verbose || curl "$gcs_copy_url" --location --output "$gcs_copy_path"
chmod +x "$gcs_copy_path"
export PATH=$PATH:"$bin_dir"
'''.split('\n')
    for path in list(input_path_uris.keys()) + list(output_path_uris.keys()):
        #code_lines.append('''mkdir -p "$(dirname "{path}")"'''.format(path=path))
        dir = str(pathlib.PurePosixPath(path).parent)
        dir = dir.replace("'", "'\''")  # escaping
        code_lines.append("""mkdir -p '{dir}'""".format(dir=dir))

    for path, uri in input_path_uris.items():
        # Escaping. Cannot/must not escape URI since it's just a placeholder
        path = path.replace("'", "'\''")  # escaping
        code_lines.append("""gcs_copy '{uri}' '{path}'""".format(path=path, uri=uri))

    code_lines.append('''"$0" "$@"''')

    for path, uri in output_path_uris.items():
        # Escaping. Cannot/must not escape URI since it's just a placeholder
        path = path.replace("'", "'\''")
        code_lines.append("""gcs_copy '{path}' '{uri}'""".format(path=path, uri=uri))

    full_command_line = [
        'sh', '-ex', '-c', ''.join(line + '\n' for line in code_lines)
    ] + user_command_line

    return full_command_line


# * ! CAIP Pipelines dropped support for sub-dags - only real type of tsk is container
# * No pipeline inputs/arguments. This makes PipelineSpec essentially a task, not component.

def _create_caip_pipeline_job_from_task_spec(
    task_spec: structures.TaskSpec,
    pipeline_root: str,
    pipeline_context: str,
) -> dict:
    root_component_spec = task_spec.component_ref.spec
    result_pipeline_spec = _create_caip_pipeline_spec_from_task_spec(task_spec)
    result_pipeline_spec['pipelineContext'] = pipeline_context

    result_pipeline_job = dict(
        spec=result_pipeline_spec,
        # I think that the unique name should not be baked in at this point.
        name='<insert fully qualified unique job name here>',
        displayName=root_component_spec.name or 'PipelineJob',
        outputPathConfig=dict(
            pipelineRoot=pipeline_root,
        ),
    )
    return result_pipeline_job


def _create_caip_pipeline_spec_from_task_spec(
    task_spec: structures.TaskSpec,
) -> dict:
    result_pipeline_steps = {}
    result_pipeline_spec = dict(
        steps=result_pipeline_steps,
        # I think that the context name should not be baked in at this point.
        #pipeline_context=...,
    )
    root_component_spec = task_spec.component_ref.spec
    root_arguments = task_spec.arguments or {}
    for input_name, argument in root_arguments.items():
        if not isinstance(argument, str):
            raise TypeError()
    if isinstance(root_component_spec.implementation, structures.GraphImplementation):
        graph_spec = root_component_spec.implementation.graph
        for task_id, task_spec in graph_spec.tasks.items():
            task_component_spec = task_spec.component_ref.spec
            task_arguments = task_spec.arguments or {}
            resolved_task_arguments = {}
            constant_task_arguments = {}
            reference_task_arguments = {}
            for input_name, argument in task_arguments.items():
                resolved_argument = None
                if isinstance(argument, str):
                    resolved_argument = argument
                    constant_task_arguments[input_name] = argument
                elif isinstance(argument, structures.GraphInputArgument):
                    resolved_argument = root_arguments[argument.graph_input.input_name]
                    constant_task_arguments[input_name] = resolved_argument
                elif isinstance(argument, structures.TaskOutputArgument):
                    resolved_argument = dict(
                        step_output=dict(
                            step=argument.task_output.task_id,
                            output=argument.task_output.output_name
                        ),
                    )
                    reference_task_arguments[input_name] = resolved_argument
                else:
                    raise TypeError()
                resolved_task_arguments[input_name] = resolved_argument
            if isinstance(task_component_spec.implementation, structures.ContainerImplementation):
                task_container = task_component_spec.implementation.container
                # Constant arguments are inlined. In future we could preserve them as property arguments
                resolved_cmd = _components._resolve_command_line_and_paths(
                    component_spec=task_component_spec,
                    #arguments=constant_task_arguments,
                    arguments=resolved_task_arguments,  # Needs to have arguments for all inputs
                )
                input_path_uris = {
                    path: "{{{{$.inputs['{}'].uri}}}}".format(input_name)
                    for input_name, path in resolved_cmd.input_paths.items()
                }
                output_path_uris = {
                    path: "{{{{$.outputs['{}'].uri}}}}".format(output_name)
                    for output_name, path in resolved_cmd.output_paths.items()
                }
                user_command_line = resolved_cmd.command + resolved_cmd.args
                full_command_line = _generate_command_line(
                    user_command_line=user_command_line,
                    input_path_uris=input_path_uris,
                    output_path_uris=output_path_uris,
                )
                result_container_dict = dict(
                    image=task_container.image,
                    #command='', # Not supported by the API yet
                    #args=full_command_line,
                    command=full_command_line,
                )
                result_task_dict = dict(
                    container=result_container_dict,
                    inputs=reference_task_arguments,
                    #{
                    #    dict(
                    #        # TODO: Might need to sanitize names
                    #        name=input_name,
                    #        value=argument_dict,
                    #    )
                    #    for input_name, argument_dict in reference_task_arguments
                    #},
                    #execution_properties={},
                    outputs={
                        output.name: dict(
                            artifact=_map_output_spec_to_artifact_spec_dict(output),
                            outputUriConfig=dict(
                                filePath=True, # Not directory
                            ),
                        )
                        for output in (task_component_spec.outputs or [])
                    }
                )
            result_pipeline_steps[task_id] = dict(
                task=result_task_dict,
                #dependencies=[...],
            )
    return result_pipeline_spec


def _map_output_spec_to_artifact_spec_dict(output):
    artifact_type = 'file'
    if output.type:
        output_type = str(output.type).lower()
        if output_type.endswith('examples') or output_type in ['csv', 'tsv']:
            artifact_type = 'dataset'
        if output_type.endswith('statistics'):
            artifact_type = 'statistics'
        if output_type.endswith('schema'):
            artifact_type = 'schema'
        if output_type.endswith('model'):
            artifact_type = 'model'
        if output_type.endswith('metrics'):
            artifact_type = 'metrics '

    custom_properties = {
        'custom:name': {'string_value': str(output.name)},
    }
    if output.type:
        custom_properties['type_name'] = {'string_value': str(output.type)}

    artifact = {
        artifact_type: {},
        'custom_properties': custom_properties,
    }
    return artifact


def compile_pipeline(
    pipeline_func: Callable,
    arguments: Dict[str, str],
    pipeline_root: str,
    pipeline_context: str = 'Default',
    #job_name: str = None,
    #project_id: str = 'managed-pipeline-test',
) -> dict:
    component_factory = components.create_graph_component_from_pipeline_func(
        pipeline_func=pipeline_func,
        embed_component_specs=True,
    )
    component_spec = component_factory.component_spec
    component_ref = structures.ComponentReference(
        spec=component_spec,
    )

    task_spec = structures.TaskSpec(
        component_ref=component_ref,
        arguments=arguments,
    )

    caip_pipeline_job = _create_caip_pipeline_job_from_task_spec(
        task_spec=task_spec,
        pipeline_root=pipeline_root,
        pipeline_context=pipeline_context,
        #full_job_name=full_job_name,
    )
    return caip_pipeline_job


def run_pipeline(
    pipeline_func: Callable,
    arguments: Dict[str, str],
    pipeline_root: str,
    pipeline_context: str = 'Default',
    job_name: str = None,
    mirror_images: bool = True,
    project_id: str = 'managed-pipeline-test',
    api_host: str = 'alpha-ml.googleapis.com',
) -> dict:
    pipeline_job = compile_pipeline(
        pipeline_func=pipeline_func,
        arguments=arguments,
        pipeline_root=pipeline_root,
        pipeline_context=pipeline_context,
    )

    if mirror_images:
        pipeline_job = _image_mirroring.mirror_and_replace_container_images(
            pipeline_job=pipeline_job,
            mirror_prefix = 'gcr.io/' + project_id + '/mirror/',
            project_id=project_id,
        )

    # Setting the job name
    if not job_name:
        job_name = 'job-' + datetime.datetime.now().isoformat()
        job_name = re.sub('[^-a-zA-Z0-9]', '-', job_name.lower()).strip('-')

    job_api = _pipeline_jobs_api.PipelineJobApi(
        project_id=project_id,
        api_host=api_host,
    )
    job = job_api.submit_job(
        pipeline_job_dict=pipeline_job,
        job_name=job_name,
    )
    logging.info('Submitted job ' + job.job_name)
    try:
        import IPython
        html = 'Job: <a href="https://pantheon.corp.google.com/ai-platform/pipelines/runs/{}?project={}&mods=caip_pipelines_eap" target="_blank" >{}</a>'.format(
            job_name, project_id, job_name,
        )
        IPython.display.display(IPython.display.HTML(html))
    except:
        pass

    return job
