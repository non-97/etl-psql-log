import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import { LambdaConstruct } from "./construct/lambda-construct";
import { EtlPsqlLogsProperty } from "../parameter/index";

export interface EtlPsqlLogsStackProperty
  extends cdk.StackProps,
    EtlPsqlLogsProperty {}

export class EtlPsqlLogStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: EtlPsqlLogsStackProperty) {
    super(scope, id, props);

    const lambdaConstruct = new LambdaConstruct(this, "LambdaConstruct", {
      ...props.logProperty,
    });
  }
}
