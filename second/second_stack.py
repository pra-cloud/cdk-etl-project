from constructs import Construct

from aws_cdk import (
    Duration,
    Stack,
    aws_s3 as s3,
    aws_glue as glue,
    aws_lambda as lambda_,
    aws_events as events,
    aws_events_targets as targets,
    aws_redshift as redshift,
    pipelines,
    aws_codepipeline as codepipeline,
    aws_codepipeline_actions as codepipeline_actions,
    aws_s3_notifications as s3_notifications,
    aws_iam as iam
)


class SecondStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, glue_role_arn: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create S3 buckets
        bucket1 = s3.Bucket(self, "Bucket10902024", bucket_name="taskbucket10902024")
        bucket2 = s3.Bucket(self, "Bucket20480987", bucket_name="leadbucket20480987")
        bucket3 = s3.Bucket(self, "Bucket334827493e3", bucket_name="activitybucket334827493e3")
        bucket_name_task = bucket1.bucket_name
        bucket_name_lead= bucket2.bucket_name
        bucket_name_activity = bucket3.bucket_name

        #####3 Pipeline with respective github repo and s3 buckets 
        
        # 1. Task pipeline
        task_pipeline = codepipeline.Pipeline(self, "TaskPipeline",
                                              pipeline_name="task-pipeline")
        source_output_task = codepipeline.Artifact()
        source_action_task = codepipeline_actions.CodeStarConnectionsSourceAction(
            action_name="github_Source",
            owner="pra-cloud",
            repo="csv-etl",
            branch="main",
            output=source_output_task,
            connection_arn="arn:aws:codestar-connections:us-east-1:851725461271:connection/f40482c7-d76b-49bc-835c-aae1c1d1fdf2"
        )
        task_pipeline.add_stage(
            stage_name="Source",
            actions=[source_action_task]
        )
        task_pipeline.add_stage(
            stage_name="Deploy",
            actions=[
                codepipeline_actions.S3DeployAction(
                    action_name="DeployAction",
                    input=source_output_task,
                    bucket=bucket1
                )
            ]
        )

        # 2. Lead pipeline
        lead_pipeline = codepipeline.Pipeline(self, "LeadPipeline",
                                              pipeline_name="lead-pipeline")
        source_output_lead = codepipeline.Artifact()
        source_action_lead = codepipeline_actions.CodeStarConnectionsSourceAction(
            action_name="github_Source",
            owner="pra-cloud",
            repo="lead-etl",
            branch="main",
            output=source_output_lead,
            connection_arn="arn:aws:codestar-connections:us-east-1:851725461271:connection/f40482c7-d76b-49bc-835c-aae1c1d1fdf2"
        )
        lead_pipeline.add_stage(
            stage_name="Source",
            actions=[source_action_lead]
        )
        lead_pipeline.add_stage(
            stage_name="Deploy",
            actions=[
                codepipeline_actions.S3DeployAction(
                    action_name="DeployAction",
                    input=source_output_lead,
                    bucket=bucket2
                )
            ]
        )

        # 3. Activity pipeline
        activity_pipeline = codepipeline.Pipeline(self, "ActivityPipeline",
                                                  pipeline_name="activity-pipeline")
        source_output_activity = codepipeline.Artifact()
        source_action_activity = codepipeline_actions.CodeStarConnectionsSourceAction(
            action_name="github_Source",
            owner="pra-cloud",
            repo="activity-etl",
            branch="main",
            output=source_output_activity,
            connection_arn="arn:aws:codestar-connections:us-east-1:851725461271:connection/f40482c7-d76b-49bc-835c-aae1c1d1fdf2"
        )
        activity_pipeline.add_stage(
            stage_name="Source",
            actions=[source_action_activity]
        )
        activity_pipeline.add_stage(
            stage_name="Deploy",
            actions=[
                codepipeline_actions.S3DeployAction(
                    action_name="DeployAction",
                    input=source_output_activity,
                    bucket=bucket3
                )
            ]
        )

    
        # This first lambda function will trigger the all 3 crawler whenever any bucket will get a code.

        three_crawler_trigger = lambda_.Function(
            self,
            "ThreeCrawlerTrigger",
            runtime=lambda_.Runtime.PYTHON_3_8,
            handler="three_crawler_trigger.lambda_handler",
            code=lambda_.Code.from_asset("lambda")
        )
 
        # Add the event notification configuration to each S3 bucket and trigger the three crawler trigger lambda function.

        for bucket in [bucket1, bucket2, bucket3]:
        # Define the S3 event notification for object creation
            s3_notification = s3_notifications.LambdaDestination(three_crawler_trigger)

            # Add the event notification configuration to the S3 bucket
            bucket.add_event_notification(
                s3.EventType.OBJECT_CREATED,
                s3_notifications.LambdaDestination(three_crawler_trigger)
        )
            
        # This second lambda function will trigger the glue job after successfull run the any one of the 3 crawler.

        second_lambda_function = lambda_.Function(
            self,
            "SecondLambdaFunction",
            runtime=lambda_.Runtime.PYTHON_3_8,
            handler="second_lambda_function.lambda_handler",
            code=lambda_.Code.from_asset("lambda")
        )

        # lead-db database for LeadCrawler

        database = glue.CfnDatabase(
            self, "MyGlueDatabase1",
            catalog_id="851725461271",
            database_input={
                "name": "lead-db"
            }
        )                          
 
        # LeadCrawler 

        lead_crawler = glue.CfnCrawler(
            self,
            "LeadCrawler",
            name="LeadCrawler",
            role=glue_role_arn,
            database_name="lead-db",
            #queue.add_depends_on(bucket)
            targets={
                "s3Targets": [
                    {"path": f"s3://{bucket_name_lead}"}
                ]
            },
        )
        

        # activity-db database for ActivityCrawler

        database = glue.CfnDatabase(
            self, "MyGlueDatabase2",
            catalog_id="851725461271",
            database_input={
                "name": "activity-db"
            }
        )
              
        # ActivityCrawler

        activity_crawler = glue.CfnCrawler(
            self,
            "ActivityCrawler",
            name="ActivityCrawler",
            role=glue_role_arn,
            database_name="activity-db",
            targets={
                "s3Targets": [
                    {"path": f"s3://{bucket_name_activity}"}
                ]
            },
        )

        # task-db database for TaskCrawler

        database = glue.CfnDatabase(
            self, "MyGlueDatabase3",
            catalog_id="851725461271",
            database_input={
                "name": "task-db"
            }
        )

        # TaskCrawler

        task_crawler = glue.CfnCrawler(
            self,
            "TaskCrawler",
            name="TaskCrawler",
            role=glue_role_arn,
            database_name="task-db",
            targets={
                "s3Targets": [
                    {"path": f"s3://{bucket_name_task}"}
                ]
            },
        )

        # here we are using depends_on to create first s3 then create crawlers.

        lead_crawler.add_depends_on(bucket1.node.default_child)
        lead_crawler.add_depends_on(bucket2.node.default_child)
        lead_crawler.add_depends_on(bucket3.node.default_child)


        # Define Glue crawlers
        crawlers = [
            "LeadCrawler",
            "ActivityCrawler",
            "TaskCrawler"
        ]

        # Create EventBridge rules for each Glue crawler when any crawler will successfully crawl then it will trigger lambda function 2 using eventbridge.

        for crawler_name in crawlers:
            # Create EventBridge rule
            rule = events.Rule(
                self,
                f"{crawler_name}Rule",
                event_pattern={
                    "source": ["aws.glue"],
                    "detail_type": ["Glue Crawler State Change"],
                    "detail": {"state": ["SUCCEEDED"], "crawlerName": [crawler_name]}
                }
            )

            # Add Lambda function as target for the rule
            rule.add_target(targets.LambdaFunction(second_lambda_function))

        # redshift_cluster 
        
        redshift_cluster = redshift.CfnCluster(self, "RedshiftCluster",
                                            cluster_type="single-node",
                                            db_name="mavirck",
    					                    master_username="admin",
                                            master_user_password="Devops123",
                                            node_type="dc2.large",
                                            multi_az=False,
                                            cluster_subnet_group_name="cluster-subnet-group-1",
                                            iam_roles=["arn:aws:iam::851725461271:role/service-role/AmazonRedshift-CommandsAccessRole-20240224T142103"]
                                            )
        
        # Set the deletion policy to 'Delete' for automatic deletion on stack deletion
        redshift_cluster.deletion_policy = "Delete"
        
        # glue job  
        
        cfn_job = glue.CfnJob(self, "MyCfnJob",
            command=glue.CfnJob.JobCommandProperty(
                name="mavrick-cdk-glue-redshift-etl",
                python_version="3",
                script_location="s3://aws-glue-assets-851725461271-ap-south-1/scripts/glue-redshift-join-job.py",
            ),
            role="arn:aws:iam::851725461271:role/service-role/AWSGlueServiceRole-apache-airflow"  # Replace with your IAM role ARN
        )
        

        
