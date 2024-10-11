package llc.nanocontext.bucketscrubber;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

public class BucketScrubber {
    private static final Logger logger = LoggerFactory.getLogger(BucketScrubber.class);

    public static void scrub(final String bucketName, final String objectPrefix) {
        final AmazonS3 amazonS3 = AmazonS3ClientBuilder.defaultClient();

        try {
            // Retrieve the list of versions. If the bucket contains more versions
            // than the specified maximum number of results, Amazon S3 returns
            // one page of results per request.
            ListVersionsRequest request = new ListVersionsRequest()
                    .withBucketName(bucketName)
                    .withMaxResults(1000);
            if (objectPrefix != null && !objectPrefix.isEmpty())
                request.withPrefix(objectPrefix);

            VersionListing versionListing = amazonS3.listVersions(request);
            while (true) {
                List<S3VersionSummary> versionSummaries = versionListing.getVersionSummaries();
                DeleteObjectsResult deleteResults = deleteObjectVersions(amazonS3, bucketName, versionSummaries);

                if (deleteResults.getDeletedObjects().size() != versionSummaries.size()) {
                    logger.warn("Tried to delete {} versions, actually deleted {}", versionSummaries.size(), deleteResults.getDeletedObjects().size());
                }

                // Check whether there are more pages of versions to retrieve. If
                // there are, retrieve them. Otherwise, exit the loop.
                if (versionListing.isTruncated()) {
                    versionListing = amazonS3.listNextBatchOfVersions(versionListing);
                } else {
                    break;
                }
            }
        } catch (AmazonServiceException e) {
            // The call was transmitted successfully, but Amazon S3 couldn't process
            // it, so it returned an error response.
            e.printStackTrace();
        } catch (SdkClientException e) {
            // Amazon S3 couldn't be contacted for a response, or the client
            // couldn't parse the response from Amazon S3.
            e.printStackTrace();
        }
    }

    /**
     *
     * @param objectSummaries
     * @return
     * @throws Exception
     */
    private static DeleteObjectsResult deleteObjectVersions(
            final AmazonS3 amazonS3,
            final String bucketName,
            final List<S3VersionSummary> objectSummaries
    ) {
        List<DeleteObjectsRequest.KeyVersion> keysToDelete = objectSummaries.stream()
                .filter(summary -> {logger.info("ObjectSummary [{}]", summary); return true;})
                .map(summary -> new DeleteObjectsRequest.KeyVersion(summary.getKey(), summary.getVersionId()))
                .collect(Collectors.toList());

        DeleteObjectsRequest deleteObjectsRequest = new DeleteObjectsRequest(bucketName)
                .withKeys(keysToDelete);
        return amazonS3.deleteObjects(deleteObjectsRequest);
    }
}
