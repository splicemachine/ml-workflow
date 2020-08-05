"""
Build the Model Deployment Dockerfile and push to container
registry
"""
import argparse
from shutil import copy as copy_file

from mlflow.models.docker_utils import _build_image as build_serving_image


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
            "COPY ./entrypoint.sh /entrypoint.sh",
            "RUN chmod +x /entrypoint.sh"
        ]
        return "\n".join(setup_commands)

    @staticmethod
    def build_docker(image_name: str):
        """
        Build the docker image
        :return:
        """
        print("Building Serving Image....")
        build_serving_image(
            image_name=image_name,
            mlflow_home=None,
            custom_setup_steps_hook=ImageBuilder.splice_serving_custom_install_hook,
            entrypoint=(
                'CMD ["sh", "/entrypoint.sh"]'
            )
        )
        print(f"Created {image_name} image!")


def main():
    """
    Main logic and CLI interface
    """
    parser = argparse.ArgumentParser(description="Build the Splice K8s Model Serving Docker Image")
    parser.add_argument('image_name', type=str,
                        help='Image name when done building (e.g. splicemachine/sm_k8_model_serving:0.0.2)')

    results = parser.parse_args()
    ImageBuilder.build_docker(image_name=results.image_name)


if __name__ == '__main__':
    main()
