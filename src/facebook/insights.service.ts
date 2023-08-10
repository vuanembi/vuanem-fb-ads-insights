import { Readable } from 'node:stream';
import { setTimeout } from 'node:timers/promises';
import axios from 'axios';

import { getClient } from './api.service';

export type ReportOptions = {
    accountId: string;
    start: string;
    end: string;
};

export type InsightsConfig = {
    level: string;
    fields: string[];
    breakdowns?: string;
};

export const get = async (options: ReportOptions, config: InsightsConfig): Promise<Readable> => {
    const client = await getClient();

    type RequestReportResponse = {
        report_run_id: string;
    };

    const requestReport = async (): Promise<string> => {
        const { accountId, start: since, end: until } = options;
        const { level, fields, breakdowns } = config;

        return client
            .request<RequestReportResponse>({
                method: 'POST',
                url: `/act_${accountId}/insights`,
                data: {
                    level,
                    fields,
                    breakdowns,
                    time_range: JSON.stringify({ since, until }),
                    time_increment: 1,
                },
            })
            .then(({ data }) => data.report_run_id);
    };

    type ReportStatusResponse = {
        async_percent_completion: number;
        async_status: string;
    };

    const pollReport = async (reportId: string): Promise<string> => {
        const data = await client
            .request<ReportStatusResponse>({ method: 'GET', url: `/${reportId}` })
            .then((res) => res.data);

        if (data.async_percent_completion === 100 && data.async_status === 'Job Completed') {
            return reportId;
        }

        if (data.async_status === 'Job Failed') {
            throw new Error(JSON.stringify(data));
        }

        await setTimeout(10_000);

        return pollReport(reportId);
    };

    type InsightsResponse = {
        data: Record<string, any>[];
        paging: { cursors: { after: string }; next: string };
    };

    const getInsights = (reportId: string): Readable => {
        const stream = new Readable({ objectMode: true, read: () => {} });

        const _getInsights = (after?: string) => {
            client
                .request<InsightsResponse>({
                    method: 'GET',
                    url: `/${reportId}/insights`,
                    params: { after, limit: 500 },
                })
                .then((response) => response.data)
                .then((data) => {
                    data.data.forEach((row) => stream.push(row));
                    const x = data.data.filter((row) => row.ad_id === '23857841935690762');

                    data.paging.next ? _getInsights(data.paging.cursors.after) : stream.push(null);
                })
                .catch((error) => {
                    if (axios.isAxiosError(error)) {
                        console.log(error.response?.data);
                    }
                    stream.emit('error', error);
                });
        };

        _getInsights();

        return stream;
    };

    return requestReport()
        .then(pollReport)
        .then(getInsights)
        .catch((err) => {
            if (axios.isAxiosError(err)) {
                console.log(JSON.stringify(err.response?.data));
            } else {
                console.log(err);
            }
            return Promise.reject(err);
        });
};
