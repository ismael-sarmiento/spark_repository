"""
    Packaging Python Projects
    Documentation: https://packaging.python.org/tutorials/packaging-projects/
"""
import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="spark_python",  # Replace with your own username
    version="0.0.1",
    author="Ismael Antonio Sarmiento Barberia",
    author_email="ismaelantonio.sarmiento@gmail.com",
    description="Project that collects applications in spark (python)",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/ismael-sarmiento/spark_repository",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
