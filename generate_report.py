import subprocess
import sys


def run_bash_command(command):
    process = subprocess.Popen(
        command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    try:
        output, error = process.communicate()
        output = output.decode("utf-8")
        error = error.decode("utf-8")
    except Exception as e:
        if command != "rm -rf reports":
            raise Exception(f"Error in command: {command}\n {e}")
    return output


# Run 'git diff --name-only "target_branch"' command and capture the output
if len(sys.argv) > 1:
    target_branch = sys.argv[1]
else:
    target_branch = "main"

git_diff_command = f"git diff --name-only {target_branch}"
output = run_bash_command(git_diff_command)

file_names = output.split("\n")

# Filter Python file names
python_files = [file for file in file_names if file.endswith(".py")]
# Generate quality report for each Python file
# print(python_files)

for file in python_files:
    filename = file.split("/")[-1].split(".")[0]
    file_location = f'reports/{file.split(".")[0].rsplit("/", 1)[0]}'
    create_path_command = f"mkdir -p {file_location}"
    ruff_command = f"ruff check {file} > {file_location}/{filename}.txt"
    run_bash_command(create_path_command)
    run_bash_command(ruff_command)

    radon_commands = [
        f"echo '\n\nRadon: Cyclomatic Complexity' >> {file_location}/{filename}.txt",
        f"radon cc {file} -s --total-average >> {file_location}/{filename}.txt",
        f"echo '\n\nRadon: Maintainability Index' >> {file_location}/{filename}.txt",
        f"radon mi {file} -s >> {file_location}/{filename}.txt",
        f"echo '\n\nRadon: Raw Metrics' >> {file_location}/{filename}.txt",
        f"radon raw {file} -s >> {file_location}/{filename}.txt",
        f"echo '\n\nRadon: Halstead complexity metrics' >> {file_location}/{filename}.txt",
        f"radon hal {file} >> {file_location}/{filename}.txt",
    ]

    for command in radon_commands:
        run_bash_command(command)
