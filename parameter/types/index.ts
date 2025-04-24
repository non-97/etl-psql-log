import * as cdk from "aws-cdk-lib";

export interface LogProperty {
  bucketName: string;
}

export interface EtlPsqlLogsProperty {
  logProperty: LogProperty;
}

export interface EtlPsqlLogsStackProperty {
  env?: cdk.Environment;
  props: EtlPsqlLogsProperty;
}
