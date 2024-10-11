package llc.nanocontext.bucketscrubber;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.CloudFormationCustomResourceEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.lambda.powertools.cloudformation.AbstractCustomResourceHandler;
import software.amazon.lambda.powertools.cloudformation.Response;

import java.util.Map;

/**
 *
 * Example CFN Template
 *   BucketScrubberStarter:
 *     Type: Custom::BucketScrubberStarter
 *     Properties:
 *       ServiceToken: arn:aws:lambda:us-east-1:665192190124:function:EmptyS3Bucket
 *       ServiceTimeout: 900
 *       StackName: !Ref 'AWS::StackName'
 *       Action: delete
 *       BucketName: bucket-to-scrub
 *       ObjectPrefix: folder
 */
public class BucketScrubberHandler extends AbstractCustomResourceHandler {
    private static final Logger logger = LoggerFactory.getLogger(BucketScrubberHandler.class);

    /**
     * Run when the resource is to be created.
     * The bucket will be scrubbed if the specified action is 'create'.
     *
     * @param event
     * @param context
     * @return
     */
    @Override
    protected Response create(CloudFormationCustomResourceEvent event, Context context) {
        logger.info("create({}, {})", event, context);

        final Map<String, Object> eventResourceProperties = event.getResourceProperties();
        final String action = eventResourceProperties == null ? null : eventResourceProperties.get("Action").toString();
        if ("create".equals(action))
            return scrub(event, context);
        else
            return Response.success(event.getPhysicalResourceId());
    }

    /**
     * Run when the resource is to be updated.
     * The bucket will be scrubbed if the specified action is 'update'.
     *
     * @param event
     * @param context
     * @return
     */
    @Override
    protected Response update(CloudFormationCustomResourceEvent event, Context context) {
        logger.info("update({}, {})", event, context);

        final Map<String, Object> eventResourceProperties = event.getResourceProperties();
        final String action = eventResourceProperties == null ? null : eventResourceProperties.get("Action").toString();
        if ("update".equals(action))
            return scrub(event, context);
        else
            return Response.success(event.getPhysicalResourceId());
    }

    /**
     * Run when the resource is to be deleted.
     * The bucket will be scrubbed if the specified action is either 'delete' or null.
     *
     * @param event
     * @param context
     * @return
     */
    @Override
    protected Response delete(CloudFormationCustomResourceEvent event, Context context) {
        logger.info("delete({}, {})", event, context);

        final Map<String, Object> eventResourceProperties = event.getResourceProperties();
        final String action = eventResourceProperties == null ? null : eventResourceProperties.get("Action").toString();
        if ("delete".equals(action) || action == null)
            return scrub(event, context);
        else
            return Response.success(event.getPhysicalResourceId());
    }

    /**
     *
     * @param event
     * @param context
     * @return
     */
    private Response scrub(CloudFormationCustomResourceEvent event, Context context) {
        final Map<String, Object> eventResourceProperties = event.getResourceProperties();
        final String bucketName = eventResourceProperties == null ? null : eventResourceProperties.get("BucketName").toString();
        final String objectPrefix = eventResourceProperties == null ? null : eventResourceProperties.get("ObjectPrefix").toString();

        if (bucketName != null) {
            BucketScrubber.scrub(bucketName, objectPrefix);
        } else {
            logger.info("No bucket name specified, ignoring ...");
        }

        return Response.success(event.getPhysicalResourceId());
    }
}
