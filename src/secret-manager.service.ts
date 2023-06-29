import { SecretManagerServiceClient } from '@google-cloud/secret-manager';

const secretManager = new SecretManagerServiceClient();

export const setSecret = async (key: string, value: string) => {
    const payload = { data: Buffer.from(value, 'utf-8') };
    
    return secretManager
        .getProjectId()
        .then((projectId) => `projects/${projectId}/secrets/${key}`)
        .then((parent) => secretManager.addSecretVersion({ parent, payload }));
};

export const getSecret = async (key: string) => {
    return secretManager
        .getProjectId()
        .then((projectId) => `projects/${projectId}/secrets/${key}/versions/latest`)
        .then((name) => secretManager.accessSecretVersion({ name }))
        .then(([res]) => res.payload?.data?.toString() || '');
};
