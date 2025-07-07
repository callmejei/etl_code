import os
import subprocess
import logging

logger = logging.getLogger("cli_logger")

def run_shell_command(cmdstr) -> str:
    """
    Runs a shell command given the command string

    Args:
        cmdstr (str): Command string which needs to be run from shell

    Returns:
        output (str): The output of the command.

    Raises:
        ValueError: raises an error in case of some exception
    """
    try:
        print(cmdstr)
        cmdstr = cmdstr.strip()
        if os.name == "posix":
            process = subprocess.Popen(
                cmdstr,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
            )

            output = process.communicate()[0]
            exitcode = process.returncode

            if exitcode == 0:
                return output, exitcode
            else:
                logger.error(f"error running the cmd: {output}")
                return output, exitcode

    except Exception as e:
        excp_str = "Exception {} occured".format(str(e))
        if logger:
            logger.error(excp_str)
        return excp_str, 1


def check_cml_env():
    return "PREPROD"
    # domain = os.getenv("CDSW_DOMAIN")

    # if not domain:
    #     return "NO_CML"

    # domain = domain.lower()
    # if domain == constants.__CML_PROD_DOMAIN:
    #     return "PROD"
    # elif domain == constants.__CML_PREPROD_DOMAIN:
    #     return "PREPROD"
    # elif domain == constants.__CML_UAT_DOMAIN:
    #     return "UAT"
    # elif domain == constants.__CML_DR_DOMAIN:
    #     return "DR"
    # else:
    #     raise Exception("Something went wrong. Unable to recognise CML env.")
