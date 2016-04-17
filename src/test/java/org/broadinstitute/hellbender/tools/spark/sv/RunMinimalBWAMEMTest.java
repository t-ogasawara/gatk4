package org.broadinstitute.hellbender.tools.spark.sv;

import org.broadinstitute.hellbender.CommandLineProgramTest;
import org.broadinstitute.hellbender.cmdline.StandardArgumentDefinitions;
import org.broadinstitute.hellbender.utils.test.ArgumentsBuilder;
import org.broadinstitute.hellbender.utils.test.BaseTest;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Tests RunMinimalBWAMEM.
 */
public final class RunMinimalBWAMEMTest extends CommandLineProgramTest {

    private static final File TEST_DATA_DIR = new File(getTestDataDir(), "spark/sv/RunMinimalBWAMEM");
    private static final Path bwaPath;
    static {
        if (System.getProperty("user.name").equalsIgnoreCase("shuang")) {
            bwaPath = Paths.get("/usr/local/bin/bwa");
        } else {
            bwaPath = Paths.get(System.getProperty("user.dir") + System.getProperty("path.separator") + "bwaApache2Build");
        }
    }

    @Test(groups="sv")
    public void testSeparate() throws IOException {

        final ArgumentsBuilder args = new ArgumentsBuilder();
        final File samOutput = boilerPlate(args);

        // input arguments
        final File input = new File(TEST_DATA_DIR, "input_1.fastq");
        args.add("-" + StandardArgumentDefinitions.INPUT_SHORT_NAME);
        args.add(input.getAbsolutePath());
        final File secondInput = new File(TEST_DATA_DIR, "input_2.fastq");
        args.add("-" + "I2");
        args.add(secondInput.getAbsolutePath());

        this.runCommandLine(args.getArgsArray());

        BufferedReader br = new BufferedReader(new FileReader(samOutput));
        Assert.assertTrue(br.readLine() != null);
    }

    @Test(groups="sv")
    public void testInterLeaved() throws IOException {

        final ArgumentsBuilder args = new ArgumentsBuilder();
        final File samOutput = boilerPlate(args);

        // input arguments
        args.add("-p");
        final File input = new File(TEST_DATA_DIR, "interleaved.fastq");
        args.add("-" + StandardArgumentDefinitions.INPUT_SHORT_NAME);
        args.add(input.getAbsolutePath());

        this.runCommandLine(args.getArgsArray());

        BufferedReader br = new BufferedReader(new FileReader(samOutput));
        Assert.assertTrue(br.readLine() != null);
    }

    private static File boilerPlate(final ArgumentsBuilder args) throws IOException{

        args.add("-" + "bwaPath");
        args.add(bwaPath.toString());

        final File wkDir = BaseTest.createTempDir("dummy");
        args.add("-" + "outDir");
        args.add(wkDir.getAbsolutePath());

        final File output = new File(wkDir, "test.sam");
        output.createNewFile();
        args.add("-" + StandardArgumentDefinitions.OUTPUT_SHORT_NAME);
        args.add(output.getName());

        final File REF = new File("/Users/shuang/Project/HG19Ref/Homo_sapiens_assembly19.fasta");
        args.add("-" + StandardArgumentDefinitions.REFERENCE_SHORT_NAME);
        args.add(REF.getAbsolutePath());

        return output;
    }
}
