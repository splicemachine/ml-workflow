"""
Build the Model Deployment Dockerfile and push to container
registry
"""
import argparse
from shutil import copy as copy_file
from mlflow.version import VERSION as v
from mlflow.models.docker_utils import _build_image as build_serving_image

SPLICE_MLFLOW_VERSION = '1.8.0'
class ImageBuilder(object):
    """
    Build our custom mlflow model serving container
    """

    @staticmethod
    def splice_serving_custom_install_hook(build_dir):
        """
        Custom Install Hook for Splice Machine
        Model Serving container
        :return: custom install steps as a string
        """
        copy_file("./entrypoint.sh", build_dir)

        setup_commands = [
            "RUN conda config --add channels conda-forge",
            "COPY ./entrypoint.sh /entrypoint.sh",
            "RUN chmod +x /entrypoint.sh",
        ]
        return "\n".join(setup_commands)

    @staticmethod
    def gcp_serving_custom_install_hook(build_dir):
        """
        Custom Install Hook for Splice Machine
        Model Serving container
        :return: custom install steps as a string
        """
        copy_file("./entrypoint_gcp.sh", build_dir)

        setup_commands = [
            "ENV CLOUDSDK_CORE_DISABLE_PROMPTS=1",
            "ENV DISABLE_NGINX=true",
            "RUN conda config --add channels conda-forge",
            "RUN apt update && apt install -y unzip",
            "RUN curl https://sdk.cloud.google.com | bash",
            "RUN mkdir -p /opt/ml",
            "COPY ./entrypoint_gcp.sh /entrypoint.sh",
            "RUN chmod +x /entrypoint.sh",
        ]
        return "\n".join(setup_commands)

    @staticmethod
    def build_docker(image_name: str, for_gcp: bool):
        """
        Build the docker image
        :return:
        """
        print("Building Serving Image....")
        build_serving_image(
            image_name=image_name,
            mlflow_home=None,
            custom_setup_steps_hook={False:ImageBuilder.splice_serving_custom_install_hook,
                                     True:ImageBuilder.gcp_serving_custom_install_hook}[for_gcp],
            entrypoint=(
                'CMD ["bash", "/entrypoint.sh"]'
            )
        )
        print(f"Created {image_name} image!")


def main():
    """
    Main logic and CLI interface
    """
    assert v == SPLICE_MLFLOW_VERSION, f'You must be running mlflow version {SPLICE_MLFLOW_VERSION} to build the image.\n' \
                                       f'If you think this is a mistake reach out to a codeowner to verify'

    parser = argparse.ArgumentParser(description="Build the Splice K8s Model Serving Docker Image")
    parser.add_argument('image_name', type=str,
                        help='Image name when done building (e.g. splicemachine/sm_k8_model_serving:0.0.1 for k8s,'
                             'splicemachine/sm_gcp_model_serving:0.0.1 for gcp run). Check newest version on dockerhub')
    parser.add_argument('-g','--for-gcp',action='store_true', help='Build image for gcp run')

    results = parser.parse_args()
    print(f'Building for GCP: {results.for_gcp}')
    ImageBuilder.build_docker(image_name=results.image_name, for_gcp=results.for_gcp)


if __name__ == '__main__':
    main()
