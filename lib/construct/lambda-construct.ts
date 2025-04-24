import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import { BaseConstructProps, BaseConstruct } from "./base-construct";
import { LogProperty } from "../../parameter";
import * as path from "path";

export interface LambdaConstructProps extends LogProperty, BaseConstructProps {}

export class LambdaConstruct extends BaseConstruct {
  readonly etlPsqlLog: cdk.aws_lambda.IFunction;

  constructor(scope: Construct, id: string, props: LambdaConstructProps) {
    super(scope, id, props);

    // IAM Policy
    const policy = new cdk.aws_iam.Policy(this, "Policy", {
      statements: [
        new cdk.aws_iam.PolicyStatement({
          effect: cdk.aws_iam.Effect.ALLOW,
          resources: ["*"],
          actions: ["xray:PutTelemetryRecords", "xray:PutTraceSegments"],
        }),
        new cdk.aws_iam.PolicyStatement({
          effect: cdk.aws_iam.Effect.ALLOW,
          resources: [
            `arn:aws:s3:::${props.bucketName}`,
            `arn:aws:s3:::${props.bucketName}/*`,
          ],
          actions: ["s3:ListBucket", "s3:GetObject", "s3:PutObject"],
        }),
      ],
    });

    // IAM Role
    const role = new cdk.aws_iam.Role(this, "Role", {
      assumedBy: new cdk.aws_iam.ServicePrincipal("lambda.amazonaws.com"),
      managedPolicies: [
        cdk.aws_iam.ManagedPolicy.fromAwsManagedPolicyName(
          "service-role/AWSLambdaBasicExecutionRole"
        ),
      ],
    });
    role.attachInlinePolicy(policy);

    // Lambda Layer
    const lambdaPowertoolsLayer =
      cdk.aws_lambda.LayerVersion.fromLayerVersionArn(
        this,
        "lambdaPowertoolsLayer",
        `arn:aws:lambda:${
          cdk.Stack.of(this).region
        }:017000801446:layer:AWSLambdaPowertoolsPythonV3-python313-arm64:4`
      );
    const duckDblayer = new cdk.aws_lambda.LayerVersion(this, "DuckDbLayer", {
      code: cdk.aws_lambda.Code.fromAsset(
        path.join(__dirname, "../src/lambda/layer"),
        {
          bundling: {
            image: cdk.aws_lambda.Runtime.PYTHON_3_13.bundlingImage,
            command: [
              "bash",
              "-c",
              "pip install -r requirements.txt -t /asset-output/python && cp -au . /asset-output/python",
            ],
          },
        }
      ),
      compatibleArchitectures: [cdk.aws_lambda.Architecture.ARM_64],
      compatibleRuntimes: [cdk.aws_lambda.Runtime.PYTHON_3_13],
    });

    // Lambda Function
    const etlPsqlLog = new cdk.aws_lambda.Function(this, "EtlPsqlLog", {
      runtime: cdk.aws_lambda.Runtime.PYTHON_3_13,
      handler: "index.lambda_handler",
      code: cdk.aws_lambda.Code.fromAsset(
        path.join(__dirname, "../src/lambda/etl_psql_logs")
      ),
      role,
      architecture: cdk.aws_lambda.Architecture.ARM_64,
      memorySize: 4096,
      ephemeralStorageSize: cdk.Size.gibibytes(3),
      timeout: cdk.Duration.seconds(300),
      tracing: cdk.aws_lambda.Tracing.ACTIVE,
      logRetention: cdk.aws_logs.RetentionDays.ONE_YEAR,
      loggingFormat: cdk.aws_lambda.LoggingFormat.JSON,
      applicationLogLevelV2: cdk.aws_lambda.ApplicationLogLevel.INFO,
      systemLogLevelV2: cdk.aws_lambda.SystemLogLevel.INFO,
      layers: [lambdaPowertoolsLayer, duckDblayer],
      environment: {
        POWERTOOLS_LOG_LEVEL: "INFO",
        POWERTOOLS_SERVICE_NAME: "etl-psql-logs",
      },
    });
    role.node.tryRemoveChild("DefaultPolicy");
    this.etlPsqlLog = etlPsqlLog;

    new cdk.aws_events.Rule(this, "Rule", {
      eventPattern: {
        source: ["aws.s3"],
        detailType: ["Object Created"],
        detail: {
          bucket: {
            name: [props.bucketName],
          },
          object: {
            key: [
              {
                wildcard: "*/raw/*/postgresql.log.*.gz",
              },
            ],
          },
        },
      },
      targets: [new cdk.aws_events_targets.LambdaFunction(etlPsqlLog)],
    });
  }
}
