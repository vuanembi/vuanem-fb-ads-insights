import { getClient } from './api.service';

type ListAccountsResponse = { data: { account_id: string; id: string }[] };

export const getAccounts = async (businessId: string) => {
    const client = await getClient();

    return Promise.all(
        ['client_ad_accounts', 'owned_ad_accounts'].map(async (edge) => {
            return client
                .request<ListAccountsResponse>({
                    method: 'GET',
                    params: { limit: 500 },
                    url: `/${businessId}/${edge}`,
                })
                .then((response) => {
                    return response.data.data;
                });
        }),
    ).then((accountGroups) => {
        return accountGroups.flatMap((accounts) => accounts.map(({ account_id }) => account_id));
    });
};
