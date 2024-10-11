package llc.nanocontext.bucketscrubber;

public class BucketScrubberTest {

    /**
     * main() runs the bucket scrubber. first arg is the bucket name, second arg is the prefix
     * @param argv
     */
    public static void main(final String[] argv) {
        String bucketName = null;
        String prefix = null;

        if (argv.length > 0)
            bucketName = argv[0];
        else {
            System.err.println("bucket name is a required arg");
            System.exit(1);
        }
        if (argv.length > 1)
            prefix = argv[1];

        BucketScrubber.scrub(bucketName, prefix);
    }
}
