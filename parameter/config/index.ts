import { EtlPsqlLogsStackProperty } from "../types";
import { logConfig } from "./log-config";

export const etlPsqlLogsStackProperty: EtlPsqlLogsStackProperty = {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION,
  },
  props: {
    logProperty: logConfig,
  },
};
