import { BigQuery } from '@google-cloud/bigquery';

const client = new BigQuery();

const DATASET = 'dev_Facebook';

type CreateLoadStreamOptions = {
    table: string;
    schema: Record<string, any>[];
};

export const createLoadStream = (options: CreateLoadStreamOptions) => {
    return client
        .dataset(DATASET)
        .table(options.table)
        .createWriteStream({
            schema: { fields: options.schema },
            sourceFormat: 'NEWLINE_DELIMITED_JSON',
            createDisposition: 'CREATE_IF_NEEDED',
            writeDisposition: 'WRITE_APPEND',
        });
};
