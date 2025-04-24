#!/usr/bin/env node
import * as cdk from "aws-cdk-lib";
import { EtlPsqlLogStack } from "../lib/etl-psql-log-stack";
import { etlPsqlLogsStackProperty } from "../parameter/index";

const app = new cdk.App();
new EtlPsqlLogStack(app, "EtlPsqlLogStack", {
  env: etlPsqlLogsStackProperty.env,
  ...etlPsqlLogsStackProperty.props,
});
