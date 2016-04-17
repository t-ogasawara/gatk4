package org.broadinstitute.hellbender.tools.spark.sv;


import java.nio.file.Path;
import java.util.ArrayList;
import java.io.IOException;
import java.util.List;

/**
 * Represents an BWA module that can be called via "run" (in base) to do actual alignment work.
 */
public final class BWAMEMModule extends CommandLineProgramModule {

    @Override
    public List<String> initializeCommands(final Path pathToProgram){
        final ArrayList<String> res = new ArrayList<>();
        res.add(pathToProgram.toString());
        res.add("mem");
        return res;
    }

    @Override
    public void setupWorkingEnvironment(final ProcessBuilder builder, final String... args){

    }
}