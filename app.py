#!/usr/bin/env python3

import aws_cdk as cdk

from second.second_stack import SecondStack


app = cdk.App()

glue_role_arn = "arn:aws:iam::851725461271:role/myglue-crawler-role"
SecondStack(app, "ProjectCdkStack", glue_role_arn=glue_role_arn)

app.synth()
