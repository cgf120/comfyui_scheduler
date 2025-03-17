from setuptools import setup, find_packages

setup(
    name="comfyui_scheduler",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "aiohttp",
        "psutil",
        "docker",
    ],
    entry_points={
        'console_scripts': [
            'comfyui-scheduler=comfyui_scheduler.main:main',
        ],
    },
)