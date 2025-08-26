
<div style="background: #fff; border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.05); padding: 24px 0; margin-bottom: 24px;">

<p align="center">
    <img src="img/ortzi_logo_nobackground.svg" alt="Ortzi Logo" width="200" style="object-fit:cover; object-position:0 0px; height:150px;"/>
</p>

<p align="center" style="font-size:1.15em; color:#333;">
    <strong>Ortzi</strong> is a practical framework for executing data analytics jobs in parallel across the cloud continuum.
</p>

</div>

It introduces the **Ortzi DataFrame API**—a familiar and intuitive programming abstraction inspired by traditional DataFrames. With Ortzi, users can seamlessly scale their local, single-threaded Python code to run efficiently in the cloud.

## The easiest way to execute this

### Pre: Install System Dependencies

Before proceeding, ensure that the following system packages are installed on your machine, as they are required to compile Ortzi's Cython extensions:

- **gcc**
- **g++**
- **make**
- **zip**

You can install these packages using your system's package manager. For example:

#### On Ubuntu/Debian:
```bash
sudo apt update
sudo apt install -y gcc g++ make zip
```

### 1. Install Ortzi and it dependencies

```bash
pip install -r requirements.txt
pip install .
```

### 2. Setup AWS Credentials


Create a file named `credentials` in the `~/.aws/` directory and add the following content:

```ini
[default]
aws_access_key_id = YOUR_ACCESS_KEY_ID
aws_secret_access_key = YOUR_SECRET_ACCESS_KEY
```

Replace `YOUR_ACCESS_KEY_ID` and `YOUR_SECRET_ACCESS_KEY` with your actual AWS credentials.


#### Optional: Add a session token

If your AWS setup requires a session token, you can include it as follows:

```ini
aws_session_token = YOUR_SESSION_TOKEN
```


### 3. (For AWS Lambda) Prepare Lithops configuration file

Ortzi uses [Lithops](https://lithops-cloud.github.io/docs/index.html) to provision cloud resources. You should set up Lithops for the backend (e.g., K8S, AWS Lambda, AWS EC2) you will be using. For AWS Lambda, follow [these steps](https://lithops-cloud.github.io/docs/source/compute_config/aws_lambda.html).

### 4. Create an AWS S3 Bucket

You will need an AWS S3 Bucket to store workload input and to exchange intermediate data between distributed tasks.

### 5. Deploy an Ortzi runtime image

Ortzi executors rely on a dedicated runtime image that includes all necessary dependencies. The script [runtime/restart_runtime.sh](runtime/restart_runtime.sh) simplifies the deployment of this runtime across various backends. For example, to deploy the runtime for AWS Lambda, use the following command:

```bash
./runtime/restart_runtime.sh aws_lambda ortzi-runtime 1.0
```

### 6. Upload necessary inputs

The [data](./data/) directory contains example input files for running workloads. Additionally, a [script to generate TPC-DS input data](./data/tpcds_gen.sh) is provided. Ensure that this data is uploaded to the AWS S3 bucket you created earlier.

### 7. Run a workload

We provide several workloads implemented with the Ortzi DataFrame API in the [examples](./examples/) directory. To execute these workloads, you need to replace the placeholder bucket names in the scripts with your own AWS S3 bucket names. For example, in the script [examples/backends/tpc-ds.py](examples/backends/tpc-ds.py), update the following lines:

```python
BUCKET = "your-input-bucket-name"
AUX_BUCKET = "your-auxiliary-bucket-name"
```

To run a TPC-DS Query 95 workload on AWS Lambda, use the following command:

```bash
./examples/backends/test-set-cf.sh tpc-ds
```


## If in doubt, contact us!

Ortzi is a research prototype, and as such, long-term maintenance is not guaranteed. If you encounter any issues while using Ortzi or replicating experiments, please feel free to reach out to us for assistance.

[![Email](https://img.shields.io/badge/Contact-Us-blue?style=for-the-badge&logo=maildotru)](mailto:germantelmo.eizaguirre@urv.cat)

## Publication

If you use this code in your research, please cite the original paper:

> **Serverless Data Analytics (Finally) Bridging the Gap: Introducing the Ortzi DataFrame**
>
> Germán T. Eizaguirre, Marc Hostau and Marc Sánchez-Artigas
>
> *2025 IEEE 18th International Conference on Cloud Computing (CLOUD)*
>
> **[Read the Paper on IEEE Xplore](https://ieeexplore.ieee.org/abstract/document/11120569)**

<details>
<summary>📄 View BibTeX Entry</summary>

```bibtex
@inproceedings{Eizaguirre2025Ortzi,
  author={Eizaguirre, Germán T. and Hostau, Marc and Sáanchez-Artigas, Marc},
  booktitle={2025 IEEE 18th International Conference on Cloud Computing (CLOUD)}, 
  title={Serverless Data Analytics (Finally) Bridging the Gap: Introducing the Ortzi DataFrame}, 
  year={2025},
  volume={},
  number={},
  pages={460-467},
  doi={10.1109/CLOUD67622.2025.00057}}

