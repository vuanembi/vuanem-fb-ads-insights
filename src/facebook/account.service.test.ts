import { getAccounts } from './account.service';

it('get-accounts', async () => {
    const businessId = '815936829385783';

    return getAccounts(businessId)
        .then((data) => {
            console.log(data);
            expect(data).toBeDefined();
        })
        .catch((error) => {
            console.error(error);
            throw error;
        });
});
