import sys
import os


_NZAE_DEBUG_LEVEL   = 0



if __name__ == "__main__":

    #  DETERMINE DEBUG LEVEL.
    for argument in sys.argv:
        if argument == "--debug":
            _NZAE_DEBUG_LEVEL = max(_NZAE_DEBUG_LEVEL, 1)
        elif argument.startswith("--debug-level"):
            _NZAE_DEBUG_LEVEL = max(int(argument[-1]), _NZAE_DEBUG_LEVEL)


    #  LOG TO STDERR OR FILE AS APPROPRIATE...
    if _NZAE_DEBUG_LEVEL > 0:

        #  LOG TO A FILE AS APPROPRIATE (AS WELL AS STDERR).
        if _NZAE_DEBUG_LEVEL > 4:
                                                                                                                                                                                                                               #  ALWAYS FLUSH.
            class FlushingFile(object):
                def __init__(self, file_path):
                    self.file_path = file_path
                def write(self, data):
                    log_file = open(self.file_path, "a")
                    log_file.write(data)
                    log_file.flush()
                    log_file.close()
                    import sys as sysModule
                    sysModule.stderr.write(data)

            import random
            logDir = "/nz/export/ae/pythonAeDebugLogs2"
            try:
               os.mkdir(logDir)
            except OSError:
               if not os.path.isdir(logDir):
                 raise
            import time
            logFileName = time.strftime("%Y%m%d-%H%M%S")
            try:
               logFileName = os.environ.get("NZAE_RUNTIME_TRANSACTION_ID", "") + "-" + logFileName
               logFileName = os.environ.get("NZAE_AE_SQL_SIGNATURE", "") + "-" + logFileName
               logFileName = logFileName.replace(" ", "").replace(",", "_")
               logFileName = logFileName.replace("(", "_").replace(")", "")
               logFileName = os.environ.get("NZAE_RUNTIME_DATA_SLICE_ID", "") + "-" + logFileName
            except Exception:
               pass
            try:
               import socket
               logFileName = socket.gethostname() + "-" + logFileName
            except Exception:
                pass
            debugLog = FlushingFile(os.path.join(logDir, logFileName + ".log"))
            debugLog.write("This log was set up by ")
            debugLog.write("\"" + os.path.abspath(__file__) + "\".\n")
                                                                                                                                                                                                                               #  OTHERWISE, JUST WRITE DEBUG OUTPUT TO STDERR.
        else:
            debugLog = sys.stderr

        #  SET THE DEBUG LOG IN THE SYS MODULE.
        sys.nzaeDebugLog = debugLog

    #  DO SOME LOGGING AS APPROPRIATE.
    sys.NZAE_DEBUG_LEVEL = _NZAE_DEBUG_LEVEL
    if _NZAE_DEBUG_LEVEL > 0:
        debugLog.write("\nAE Started.\n")
        debugLog.write("Command Line: " + sys.executable + " " + " ".join(sys.argv) + "\n")
        if _NZAE_DEBUG_LEVEL > 4:
            debugLog.write("Environment:\n")
            for name, value in sorted(os.environ.items()):
                debugLog.write("  " + name + "=" + (max(0, 30-len(name)) * " ") + value + "\n")
        debugLog.write("\nImporting nzae...\n")  #  SET UP THE PYTHONPATH TO .../lib/python and .../lib/$HOST_OR_SPU/python.
    projectDir  = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    hostOrSpu   = os.environ.get("HOST_OR_SPU")
    if hostOrSpu not in ["host64", "spu64"]:
        raise Exception("The 'HOST_OR_SPU' environment variable must be set to 'spu64' or 'host64'.")
    nzaeDir             = os.path.join(projectDir, "lib", "python")
    nzaeHostOrSpuDir    = os.path.join(projectDir, "lib", hostOrSpu, "python")
    sys.path            = [nzaeHostOrSpuDir, nzaeDir] + sys.path

    #  IMPORT nzae MODULE.
    try:
        import nzae
    except Exception as exception:
        if _NZAE_DEBUG_LEVEL > 0:
            debugLog.write("\nException encountered trying to import nzae module:\n")
            debugLog.write("sys.path:\n")
            debugLog.write("\n".join(sys.path) + "\n\n")
            debugLog.write("Exception:\n" + str(exception) + "\n")
        raise
    if _NZAE_DEBUG_LEVEL > 0:
        debugLog.write("Done importing nzae.\n")

    try:

        #  INITIALIZE.
        try:
            if _NZAE_DEBUG_LEVEL > 0:
                debugLog.write("calling nzae.Ae._initializeInternal()...")
            nzae.Ae._initializeClass()
            if _NZAE_DEBUG_LEVEL > 0:
                debugLog.write(" done.\n")
        except Exception as exception:
            try:
                nzae.Ae._handleException(exception)
            except Exception as exception:
                if _NZAE_DEBUG_LEVEL > 0:
                    debugLog.write("Error occurred calling nzae.Ae._handleException():")
                    debugLog.write(str(exception))
                raise
        try:

            #  DETERMINE THE PYTHON FILE TO "IMPORT".
            #if len(sys.argv) < 2:
                #arguments = str(sys.argv)
                #nzae.Ae.error("Not enough arguments to launchAeudtf.py: " + arguments[1:])
                #raise
            code_to_execute = os.environ.get("CODE_TO_EXECUTE")

            #  "exec" THE REGISTERED PYTHON FILE.
            if _NZAE_DEBUG_LEVEL > 0:
                debugLog.write("Executing \"" +code_to_execute + "\"...\n")
            exec(compile(code_to_execute, 'client.py', 'exec'))
            if _NZAE_DEBUG_LEVEL > 0:
                debugLog.write("Done executing \"" +code_to_execute + "\".\n")

            #  VERIFY THAT THE CLASS WAS ACTUALLY RUN.
            if not nzae.Ae.didClassRun():
                raise Exception("The AE in '" + code_to_execute + "' was never run.")

        except Exception as exception:
            if _NZAE_DEBUG_LEVEL > 0:
                debugLog.write(" ERROR.\n")
            raise

        #  CLEAN UP THE AE CLASS.
        finally:
            if _NZAE_DEBUG_LEVEL > 0:
                debugLog.write("Cleaning up the class...")
            nzae.Ae._cleanUpClass()
            if _NZAE_DEBUG_LEVEL > 0:
                 debugLog.write(" done.\n")

    #  HANDLE EXCEPTIONS.
    except Exception as exception:
            if _NZAE_DEBUG_LEVEL > 0:
                debugLog.write("ERROR: Exception encountered:\n")
                debugLog.write(str(exception) + "\n")
            try:
                nzae.Ae._handleException(exception, shouldExit=False)
            except Exception as exception:
                if _NZAE_DEBUG_LEVEL > 0:
                    debugLog.write("Error occurred calling nzae.Ae._handleException():")
                    debugLog.write(str(exception))
                raise
            finally:
                if _NZAE_DEBUG_LEVEL > 0:
                  debugLog.write("Cleaning up the class...")
                nzae.Ae._cleanUpClass()
                if _NZAE_DEBUG_LEVEL > 0:
                 debugLog.write(" done.\n")





