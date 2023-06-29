import express from 'express';
import Joi from 'joi';
import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc';

dayjs.extend(utc);

import * as LoggingService from './logging.service';
import { AUTH_ROUTE, AUTH_CALLBACK_ROUTE, authenticate, getAuthUrl } from './facebook/auth.service';
import * as pipelines from './facebook/pipeline.const';
import { runPipeline, createPipelineTasks } from './facebook/pipeline.service';

const app = express();

app.use(express.json());

app.use(({ params, path, body }, _, next) => {
    LoggingService.info({ path, params, body });
    next();
});

app.get(AUTH_CALLBACK_ROUTE, (req, res) => {
    authenticate(req.query.code as string)
        .then((result) => res.status(200).json({ result }))
        .catch((error) => res.status(500).json({ error }));
});

app.get(AUTH_ROUTE, (_, res) => {
    res.redirect(getAuthUrl());
});

type CreatePipelineTasksBody = {
    start: string;
    end: string;
};

app.use('/task', (req, res) => {
    Joi.object<CreatePipelineTasksBody>({
        start: Joi.string()
            .optional()
            .empty(null)
            .allow(null)
            .default(dayjs.utc().subtract(7, 'day').format('YYYY-MM-DD')),
        end: Joi.string()
            .optional()
            .empty(null)
            .allow(null)
            .default(dayjs.utc().format('YYYY-MM-DD')),
    })
        .validateAsync(req.body)
        .then((body) =>
            createPipelineTasks(body)
                .then((result) => {
                    res.status(200).json({ result });
                })
                .catch((error) => {
                    console.error(JSON.stringify(error));
                    res.status(500).json({ error });
                }),
        )
        .catch((error) => {
            console.error(JSON.stringify(error));
            res.status(500).json({ error });
        });
});

type RunPipelineBody = {
    accountId: string;
    start: string;
    end: string;
    pipeline: keyof typeof pipelines;
};

app.use('/', (req, res) => {
    Joi.object<RunPipelineBody>({
        accountId: Joi.string(),
        start: Joi.string(),
        end: Joi.string(),
        pipeline: Joi.string(),
    })
        .validateAsync(req.body)
        .then(async ({ pipeline, accountId, start, end }) => {
            return runPipeline({ accountId, start, end }, pipelines[pipeline])
                .then((result) => {
                    res.status(200).json({ result });
                })
                .catch((error) => {
                    console.error(JSON.stringify(error));
                    res.status(500).json({ error });
                });
        })

        .catch((error) => {
            console.error(JSON.stringify(error));
            res.status(500).json({ error });
        });
});

app.listen(8080);
