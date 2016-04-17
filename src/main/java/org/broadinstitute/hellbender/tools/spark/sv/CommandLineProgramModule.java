package org.broadinstitute.hellbender.tools.spark.sv;

import com.google.common.annotations.VisibleForTesting;
import scala.Tuple2;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * A class for experimenting with cmd line programs (such as bwa and sga) that's not part of GATK (or has Java bindings yet).
 */
public abstract class CommandLineProgramModule {

    public CommandLineProgramModule(){  }

    /**
     * Builds the initial argument list for the command line (e.g. "bwa mem").
     * @return a list of strings having the names of requested tools
     */
    abstract List<String> initializeCommands(final Path pathToProgram);

    /**
     * Function to execute the command line program, if available, on the provided arguments.
     * @param runtimeArguments          arguments to be provided to the program; can be null (e.g. program like "ls" taking no arguments)
     * @param directoryToWorkIn         a directory for the program to work in (mainly for output); can be null
     * @param stdoutDestination         redirects the stdout from the program to a file; can be null, but stdout message by program will be lost
     * @param stderrDestination         redirects the stderr message to a string; can be null, but stderr message by program may be lost
     * @param workingEnvironmentArgs    arguments to setup working environment for the program to run under
     * @return                          exit status of running the process, paired with error message string (empty if int is 0)
     *                                  Several possible values for the returned integer value:
     *                                  0 -- successfully executed the process
     *                                  1 -- program failed to start
     *                                  2 -- program interrupted so cannot finish successfully
     *                                  10+ -- exit status of underlying program, padded by 10
     */
    public Tuple2<Integer, String> run(final Path pathToProgram,
                                       final List<String> runtimeArguments,
                                       final File directoryToWorkIn,
                                       final File stdoutDestination,
                                       final File stderrDestination,
                                       final String... workingEnvironmentArgs) {

        List<String> commands = initializeCommands(pathToProgram);
        if(null!=runtimeArguments && !runtimeArguments.isEmpty()){
            commands.addAll(runtimeArguments);
        }

        final ProcessBuilder builder = new ProcessBuilder(commands);

        setupWorkingEnvironment(builder, workingEnvironmentArgs);
        redirectIO(builder, directoryToWorkIn, stdoutDestination, stderrDestination);

        try{
            String stderrMessage = "";
            int exitStatus = 0;
            if(null!=stderrDestination){    // user redirects stderr to file, error message handling is user's responsibility
                Process runProcess = builder.start();
                exitStatus = runProcess.waitFor();
            }else{                          // user decides not to log stderr, but we want to log anyway, in case things go wrong
                Process runProcess = builder.start();
                try(BufferedReader reader = new BufferedReader(new InputStreamReader(runProcess.getErrorStream()))) {
                    exitStatus = runProcess.waitFor();
                    StringBuilder stringBuilder = new StringBuilder();
                    String line;
                    while ( (line = reader.readLine()) != null) {
                        stringBuilder.append(line);
                        stringBuilder.append(System.getProperty("line.separator"));
                    }
                    stderrMessage = stringBuilder.toString();
                }
            }

            if(0!=exitStatus){
                return new Tuple2<>(10+exitStatus,
                                    String.join(" ", commands) + System.getProperty("line.separator") + stderrMessage);
            }
        } catch (final IOException e){
            String newErrMess = e.getMessage();
            if(null!=e.getCause()){
                newErrMess += e.getCause().toString();
            }
            return new Tuple2<>(1, newErrMess);
        } catch (final InterruptedException e){
            String newErrMess = e.getMessage();
            if(null!=e.getCause()){
                newErrMess += e.getCause().toString();
            }
            return new Tuple2<>(2, newErrMess);
        }

        return new Tuple2<>(0, "");
    }

    /**
     * Setts up the working directory, redirects the stdout and stderr messages to user-provided strings.
     * @param builder           process builder for the program
     * @param directoryToWorkIn directory to work in
     * @param stdoutDestination stdout destination provided by user
     * @param stderrDestination stderr destination provided by user
     */
    protected void redirectIO(final ProcessBuilder builder,
                              final File directoryToWorkIn,
                              final File stdoutDestination,
                              final File stderrDestination){
        if(null!=directoryToWorkIn){
            builder.directory(directoryToWorkIn);
        }
        if(null!=stdoutDestination){
            builder.redirectOutput(stdoutDestination);
        }
        if(null!=stderrDestination){
            builder.redirectError(stderrDestination);
        }
    }

    /**
     * Sets up working environment for the program.
     * Inheriting classes can, and most likely should, override to do non-trivial work.
     * @param builder process builder for the program
     * @param args    arguments used for setting up the environment
     */
    protected void setupWorkingEnvironment(final ProcessBuilder builder, final String... args){

    }
}
