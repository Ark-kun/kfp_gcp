import datetime
import requests
import subprocess
import time
import urllib


def _gcloud_get_access_token():
    access_token = subprocess.run(
        ['gcloud', 'auth', 'print-access-token'],
        capture_output=True,
        check=True,
    ).stdout.decode('utf-8').strip()
    return access_token


def _gcloud_http_get_json(url: str) -> None:
    access_token = _gcloud_get_access_token()
    response = requests.get(
        url=url,
        headers={
            'Authorization': 'Bearer ' + access_token,
        },
    )
    if response.status_code >= 400:
        print('response.status_code=' + str(response.status_code))
        print('response.content=' + response.content.decode(response.encoding))
        print('response.json=' + str(response.json()))
    response.raise_for_status()
    return response.json()


def _gcloud_http_post_json(url: str, json) -> None:
    access_token = _gcloud_get_access_token()
    response = requests.post(
        url=url,
        headers={
            'Authorization': 'Bearer ' + access_token,
        },
        json=json,
    )
    if response.status_code >= 400:
        print('response.status_code=' + str(response.status_code))
        print('response.content=' + response.content.decode(response.encoding))
        print('response.json=' + str(response.json()))
    response.raise_for_status()
    return response.json()


class _PipelineJob:
    def __init__(
        self,
        api: '_PipelineJobApi',
        job_name: str,
    ):
        self.api = api
        self.job_name = job_name
        self.current_state = {}
    
    def cancel(self) -> None:
        self.api.cancel(self.job_name)
    
    def refresh(self) -> None:
        self.current_state = self.api.get_job_json(self.job_name)

    def wait_for_completion(
        self,
        timeout: datetime.timedelta = datetime.timedelta.max,
        interval_seconds: float = 20,
    ) -> None:
        #states = ['PENDING', 'RUNNING', 'SUCCEEDED', 'FAILED', 'TIMEOUT', 'CANCELLING', 'CANCELLED']
        active_states = ['PENDING', 'RUNNING', 'CANCELLING']
        #stopped_states = ['SUCCEEDED', 'FAILED', 'CANCELLED']

        start_time = datetime.datetime.utcnow()
        while True:
            current_time = datetime.datetime.utcnow()
            if current_time - start_time > timeout:
                raise TimeoutError()
            
            try:
                self.refresh()
            except Exception as ex:
                #print(ex)
                time.sleep(interval_seconds)
                continue

            job_state = self.current_state.get('state')
            if not job_state:
                print(self.current_state)
                return

            task_executions = self.current_state.get('jobDetail', {}).get('taskExecutions', [])
            execution_states = {execution['step']: execution['state'] for execution in task_executions}
            print(job_state + ': ' + str(execution_states))

            if job_state not in active_states:
                return
            time.sleep(interval_seconds)
    
    def __str__(self):
        return '_PipelineJob(job_name={})'.format(self.job_name)


class PipelineJobApi:
    def __init__(
        self,
        project_id: str = 'managed-pipeline-test',
        api_host: str = 'test-ml.sandbox.googleapis.com',
    ):
        url_prefix = 'https://{api_host}/v1/projects/{project_id}/pipelineJobs'.format(
            api_host=api_host,
            project_id=project_id,
        )
        self.api_host = api_host
        self.project_id = project_id
        self.url_prefix = url_prefix
    
    def get_all_jobs_json(self) -> dict:
        # returns {"pipelineJobs": [...]}
        response_json = _gcloud_http_get_json(self.job_url)
        self.current_state = response_json
    
    def get_job_url(self, job_name: str) -> str:
        return self.url_prefix + '/' + urllib.parse.quote(job_name)
    
    def get_job_object(self, job_name: str) -> _PipelineJob:
        return _PipelineJob(api=self, job_name=job_name)
    
    def get_job_json(self, job_name: str) -> dict:
        return _gcloud_http_get_json(self.get_job_url(job_name))

    def cancel(self, job_name: str) -> None:
        url = self.get_job_url(job_name) + ':cancel'
        response_json = _gcloud_http_get_json(url)
        return response_json
    
    def submit_job(self, pipeline_job_dict: dict, job_name: str) -> _PipelineJob:
        full_job_name = 'projects/{project_id}/pipelineJobs/{job_name}'.format(
            project_id=self.project_id,
            job_name=job_name,
        )
        pipeline_job_dict['name'] = full_job_name

        _gcloud_http_post_json(
            url=self.url_prefix,
            json=pipeline_job_dict,
        )
        return _PipelineJob(api=self, job_name=job_name)   
