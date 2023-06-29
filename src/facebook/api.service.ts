import axios from 'axios';

import { getSecret } from '../secret-manager.service';
import { API_VERSION } from './facebook.const';

export const getClient = async () => {
    const accessToken = await getSecret('facebook-user-token');

    return axios.create({
        baseURL: `https://graph.facebook.com/${API_VERSION}`,
        params: { access_token: accessToken },
    });
};
