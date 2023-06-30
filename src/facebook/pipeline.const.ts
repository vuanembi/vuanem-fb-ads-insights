import Joi from 'joi';

import { InsightsConfig } from './insights.service';

export type Pipeline = {
    name: string;
    insightsConfig: InsightsConfig;
    validationSchema: Joi.Schema;
    schema: Record<string, any>[];
};

const actionBreakdownSchema = Joi.array()
    .items({ action_type: Joi.string(), value: Joi.number() })
    .optional();

export const ADS_INSIGHTS: Pipeline = {
    name: 'AdsInsights',
    insightsConfig: {
        level: 'ad',
        fields: [
            'date_start',
            'date_stop',
            'account_id',
            'account_name',
            'campaign_id',
            'campaign_name',
            'adset_id',
            'adset_name',
            'ad_id',
            'ad_name',
            'clicks',
            'cpc',
            'cpm',
            'ctr',
            'impressions',
            'reach',
            'spend',
            'action_values',
            'actions',
            'cost_per_action_type',
            'cost_per_unique_action_type',
        ],
    },
    validationSchema: Joi.object({
        date_start: Joi.string(),
        date_stop: Joi.string(),
        account_id: Joi.number().unsafe(),
        account_name: Joi.string(),
        campaign_id: Joi.number().unsafe(),
        campaign_name: Joi.string(),
        adset_id: Joi.number().unsafe(),
        adset_name: Joi.string(),
        ad_id: Joi.number().unsafe(),
        ad_name: Joi.string(),
        clicks: Joi.number().optional(),
        cpc: Joi.number().optional(),
        cpm: Joi.number().optional(),
        ctr: Joi.number().optional(),
        impressions: Joi.number().optional(),
        reach: Joi.number().optional(),
        spend: Joi.number().optional(),
        action_values: actionBreakdownSchema.optional(),
        actions: actionBreakdownSchema.optional(),
        cost_per_action_type: actionBreakdownSchema.optional(),
        cost_per_unique_action_type: actionBreakdownSchema.optional(),
    }),
    schema: [
        { name: 'date_start', type: 'DATE' },
        { name: 'date_stop', type: 'DATE' },
        { name: 'account_id', type: 'NUMERIC' },
        { name: 'account_name', type: 'STRING' },
        { name: 'campaign_id', type: 'NUMERIC' },
        { name: 'campaign_name', type: 'STRING' },
        { name: 'adset_id', type: 'NUMERIC' },
        { name: 'adset_name', type: 'STRING' },
        { name: 'ad_id', type: 'NUMERIC' },
        { name: 'ad_name', type: 'STRING' },
        { name: 'clicks', type: 'NUMERIC' },
        { name: 'cpc', type: 'NUMERIC' },
        { name: 'cpm', type: 'NUMERIC' },
        { name: 'ctr', type: 'NUMERIC' },
        { name: 'impressions', type: 'NUMERIC' },
        { name: 'reach', type: 'NUMERIC' },
        { name: 'spend', type: 'NUMERIC' },
        {
            name: 'action_values',
            type: 'RECORD',
            mode: 'REPEATED',
            fields: [
                { name: 'action_type', type: 'STRING' },
                { name: 'value', type: 'NUMERIC' },
            ],
        },
        {
            name: 'actions',
            type: 'RECORD',
            mode: 'REPEATED',
            fields: [
                { name: 'action_type', type: 'STRING' },
                { name: 'value', type: 'NUMERIC' },
            ],
        },
        {
            name: 'cost_per_action_type',
            type: 'RECORD',
            mode: 'REPEATED',
            fields: [
                { name: 'action_type', type: 'STRING' },
                { name: 'value', type: 'NUMERIC' },
            ],
        },
        {
            name: 'cost_per_unique_action_type',
            type: 'RECORD',
            mode: 'REPEATED',
            fields: [
                { name: 'action_type', type: 'STRING' },
                { name: 'value', type: 'NUMERIC' },
            ],
        },
    ],
};
