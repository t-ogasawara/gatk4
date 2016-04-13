package org.broadinstitute.hellbender.tools.spark.sv;

import com.google.common.annotations.VisibleForTesting;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

/**
 * A class for experimenting with cmd line programs (such as bwa and sga) that's not part of GATK (or has Java bindings yet).
 */
public abstract class CMDLineProgramModule {

    public CMDLineProgramModule(){  }

    /**
     * Builds the initial argument list for the command line (e.g. "bwa mem").
     * @return a list of strings having the names of requested tools.
     * @throws IOException
     */
    abstract ArrayList<String> initializeCommands() throws IOException;

    /**
     * Worker function to call the cmd line program.
     * @param runtimeArguments          arguments to be provided to the program
     * @param directoryToWorkIn         a directory for the program to work in (mainly for output)
     * @param stdoutDestination         redirects the stdout from the program to a file
     * @param stderrMessageDestination  appends the stderr message to a string, which is assumed to be not null
     * @throws IOException
     * @throws InterruptedException
     * @throws RuntimeException
     */
    public void run(final ArrayList<String> runtimeArguments,
                    final File directoryToWorkIn,
                    final File stdoutDestination,
                    String stderrMessageDestination) throws IOException, InterruptedException, RuntimeException{

        final ArrayList<String> commands = initializeCommands();
        for(final String arg : runtimeArguments){ commands.add(arg); }

        final ProcessBuilder builder = new ProcessBuilder(commands);
        setupWorkingDir(builder, directoryToWorkIn, stdoutDestination);
        setupWorkingEnvironment(builder);

        Process runProcess = builder.start();
        int exitStatus = runProcess.waitFor();
        stderrMessageDestination += redirectSTDERR(runProcess);

        if(0!=exitStatus){ onError(commands, stderrMessageDestination, exitStatus); }
    }

    protected void setupWorkingDir(final ProcessBuilder builder,
                                   final File directoryToWorkIn,
                                   final File stdoutDestination){
        builder.directory(directoryToWorkIn);
        if(null!=stdoutDestination) { builder.redirectOutput(stdoutDestination); }
    }

    /**
     * Sets up working environment for the program. Inheriting classes should provide implementation.
     * @param builder process builder for the program
     * @param args    arguments used for setting up the environment
     */
    abstract void setupWorkingEnvironment(final ProcessBuilder builder, final String... args);

    /**
     * Redirects the stderr message from the represented program to a string.
     * @param process       the program's process
     * @return              String converted from the stderr message of the program
     * @throws IOException
     */
    protected static String redirectSTDERR(final Process process) throws IOException{
        final BufferedReader reader = new BufferedReader(new InputStreamReader(process.getErrorStream()));
        StringBuilder out = new StringBuilder();
        String line       = null;
        String previous   = null;
        while ((line = reader.readLine()) != null) {
            if (!line.equals(previous)) {
                previous = line;
                out.append(line).append('\n');
            }
        }
        return out.toString();
    }

    /**
     * Strategy for error handling when program returns non-zero exit status.
     * @param commands           the list of commands, arguments, flags used in running the process
     * @param stderrMessage      the stderr message form the process
     * @param commandExitStatus  exit status of the process
     * @throws RuntimeException
     */
    private static void onError(final ArrayList<String> commands, final String stderrMessage, final int commandExitStatus) throws RuntimeException{
        String errorMessage = "";
        for(final String mess : commands){ errorMessage += " " + mess; }
        errorMessage += "\n" + stderrMessage;
        throw new RuntimeException("Errors occurred while running: " + errorMessage +
                                    "\nWith exit status: " + commandExitStatus);
    }

    /**
     * Checks if the program to be represented is available.
     * @param programName   the name of the program (case-sensitive) to be tested and run subsequently if available
     * @throws IOException  throws IOException if the program is not available.
     */
    @VisibleForTesting
    static void checkIfProgramIsAvailableOnHost(final String programName) throws IOException{
        try{
            final ArrayList<String> commands = new ArrayList<>();
            commands.add("which");
            commands.add(programName);
            final ProcessBuilder testIfProgramIsAvailableOnHost = new ProcessBuilder(commands);
            final Process programPath = testIfProgramIsAvailableOnHost.start();
            int exitStatus = programPath.waitFor();
            if(0!=exitStatus){
                throw new IOException("Can't find " + programName + " programs in $PATH of host.");
            }
        } catch(final InterruptedException e){
            throw new IOException("Errors occurred while testing where " + programName + " lives. " + e.getMessage());
        }
    }
}
