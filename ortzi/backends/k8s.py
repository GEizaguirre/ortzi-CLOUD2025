import types
import yaml
import logging
import json

import pika
from lithops.serverless.backends.k8s import config
from lithops import FunctionExecutor, __version__, utils
from kubernetes.client.rest import ApiException


logger = logging.getLogger(__name__)


def invoke(self, docker_image_name, runtime_memory, job_payload):

    """
    Invoke -- return information about this invocation
    For array jobs only remote_invocator is allowed
    """

    if self.rabbitmq_executor:
        self.image = docker_image_name
        config_changed = self._has_config_changed(runtime_memory)

        if config_changed:
            logger.debug("Waiting for kubernetes to change the configuration")
            self._delete_workers()
            self._create_workers(runtime_memory)

        # First init
        elif self.current_runtime != self.image:
            self._create_workers(runtime_memory)

        job_key = job_payload['job_key']
        self.jobs.append(job_key)

        # Send packages of tasks to the queue
        granularity = max(1, job_payload['total_calls'] // len(self.nodes)
                            if self.k8s_config['worker_processes'] <= 1 else self.k8s_config['worker_processes'])

        times, res = divmod(job_payload['total_calls'], granularity)

        for i in range(times + (1 if res != 0 else 0)):
            num_tasks = granularity if i < times else res
            payload_edited = job_payload.copy()

            start_index = i * granularity
            end_index = start_index + num_tasks

            payload_edited['call_ids'] = payload_edited['call_ids'][start_index:end_index]
            payload_edited['data_byte_ranges'] = payload_edited['data_byte_ranges'][start_index:end_index]
            payload_edited['total_calls'] = num_tasks

            self.channel.basic_publish(
                exchange='',
                routing_key='task_queue',
                body=json.dumps(payload_edited),
                properties=pika.BasicProperties(
                    delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
                ))

        activation_id = f'lithops-{job_key.lower()}'
    else:
        master_ip = self._start_master(docker_image_name)

        max_workers = job_payload['max_workers']
        executor_id = job_payload['executor_id']
        job_id = job_payload['job_id']

        job_key = job_payload['job_key']
        self.jobs.append(job_key)

        total_calls = job_payload['total_calls']
        chunksize = job_payload['chunksize']
        total_workers = min(max_workers, total_calls // chunksize + (total_calls % chunksize > 0))

        logger.debug(
            f'ExecutorID {executor_id} | JobID {job_id} - Required Workers: {total_workers}'
        )

        activation_id = f'lithops-{job_key.lower()}'

        job_res = yaml.safe_load(config.JOB_DEFAULT)
        job_res['metadata']['name'] = activation_id
        job_res['metadata']['namespace'] = self.namespace
        job_res['metadata']['labels']['version'] = 'lithops_v' + __version__
        job_res['metadata']['labels']['user'] = self.user

        job_res['spec']['activeDeadlineSeconds'] = self.k8s_config['runtime_timeout']
        job_res['spec']['parallelism'] = total_workers

        container = job_res['spec']['template']['spec']['containers'][0]
        container['image'] = docker_image_name
        if not docker_image_name.endswith(':latest'):
            container['imagePullPolicy'] = 'IfNotPresent'

        container['env'][0]['value'] = 'run_job'
        container['env'][1]['value'] = utils.dict_to_b64str(job_payload)
        container['env'][2]['value'] = master_ip

        container['resources']['requests']['memory'] = f'{runtime_memory}Mi'
        container['resources']['requests']['cpu'] = str(self.k8s_config['runtime_cpu'])
        container['resources']['limits']['memory'] = f'{runtime_memory}Mi'
        container['resources']['limits']['cpu'] = str(self.k8s_config['runtime_cpu'])

        shared_memory_size = min(512, runtime_memory // 2)  # Set shared memory size to half of the runtime memory, max 512Mi
        shm_volume = {
            'name': 'dshm',  # Name of the volume
            'emptyDir': {
                'medium': 'Memory',      # Specifies that the volume should be RAM-backed
                'sizeLimit': f'{shared_memory_size}Mi'   # Sets the size limit for the shared memory
            }
        }

        if 'volumes' not in job_res['spec']['template']['spec']:
            job_res['spec']['template']['spec']['volumes'] = []
        job_res['spec']['template']['spec']['volumes'].append(shm_volume)
        logger.info(f'ExecutorID {executor_id} | JobID {job_id} - '
                        f'Shared memory size set to {shared_memory_size}Mi')

        shm_volume_mount = {
            'name': 'dshm',          # Must match the volume's name
            'mountPath': '/dev/shm'  # Mount path inside the container
        }

        if 'volumeMounts' not in container:
            container['volumeMounts'] = []
        container['volumeMounts'].append(shm_volume_mount)

        logger.debug(f'ExecutorID {executor_id} | JobID {job_id} - Going '
                        f'to run {total_calls} activations in {total_workers} workers')

        if not all(key in self.k8s_config for key in ["docker_user", "docker_password"]):
            del job_res['spec']['template']['spec']['imagePullSecrets']

        try:
            self.batch_api.create_namespaced_job(
                namespace=self.namespace,
                body=job_res
            )
        except ApiException as e:
            raise e

    return activation_id


def patch_k8s_invoker(
    executor: FunctionExecutor
):

    executor.compute_handler.backend.invoke = types.MethodType(
        invoke,
        executor.compute_handler.backend
    )
