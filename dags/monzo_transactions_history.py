import datetime
import os
from json import loads

from airflow.decorators import dag, task


class MonzoException(Exception):
    pass


default_args = {
    "owner": "airflow",
}


@dag(
    default_args=default_args,
    start_date=datetime.datetime(2022, 1, 1),
    catchup=False,
    max_active_runs=1,
    schedule=None,
)
def monzo_transactions_history():
    """
    DAG to pull all Monzo transactions using Monzo API package.
    """

    @task()
    def get_monzo_auth_history():
        """
        Task to create Monzo authentication object

        Returns:
            Authenticaton object with tokens
        """
        from monzo.authentication import Authentication
        from monzo.handlers.filesystem import FileSystem

        with open(
            os.getenv("MONZO_TOKENS_JSON"),
            "r",
        ) as tokens:
            content = loads(tokens.read())

        monzo_auth_obj = Authentication(
            client_id=os.getenv("MONZO_CLIENT_ID"),
            client_secret=os.getenv("MONZO_CLIENT_SECRET"),
            redirect_url=os.getenv("MONZO_REDIRECT_URL"),
            access_token=content["access_token"],
            access_token_expiry=content["expiry"],
            refresh_token=content["refresh_token"],
        )
        handler = FileSystem(os.getenv("MONZO_TOKENS_JSON"))
        monzo_auth_obj.register_callback_handler(handler)

        return monzo_auth_obj

    @task()
    def get_monzo_transactions_history(monzo_auth_obj):
        """
        Task to pull all transactions from Monzo

        Args:
            monzo_auth_object: Authentication object with tokens

        Returns:
            Path to saved transactions JSON file
        """
        import pandas as pd
        from monzo_transactions.fetch_transactions import FetchTransactions
        from monzo.endpoints.pot import Pot
        from monzo.endpoints.account import Account
        from monzo_transactions.source_accounts import POT_ACCOUNTS, MAIN_ACCOUNT

        transactions_path = os.getenv("MONZO_TRANSACTIONS_JSON")

        trn = FetchTransactions(monzo_auth_obj)

        all_transactions = []

        main_accounts = Account.fetch(monzo_auth_obj)

        for acc in main_accounts:
            print(acc.account_id, acc.description)
            main_transactions = trn.fetch_transactions(
                account_id=acc.account_id, created_date=acc.created, history=True
            )
            all_transactions.append(main_transactions)

        pot_obj_lst = Pot.fetch(auth=monzo_auth_obj, account_id=MAIN_ACCOUNT)

        pots_lst_filter = [
            pot for pot in pot_obj_lst if pot.pot_id in POT_ACCOUNTS.keys()
        ]
        for p in pots_lst_filter:
            print(p.pot_id)
        for live_pot in pots_lst_filter:
            pot_account_id = POT_ACCOUNTS.get(live_pot.pot_id)
            print(pot_account_id, live_pot.name)
            pot_transactions = trn.fetch_transactions(
                account_id=pot_account_id, created_date=live_pot.created, history=True
            )
            all_transactions.append(pot_transactions)

        transactions = pd.concat(all_transactions)
        transactions.to_json(transactions_path, orient="records")

        return transactions_path

    @task()
    def upload_monzo_transactions_history(transactions_path):
        """
        Task to upload transactions to database

        Args:
            transactions_path: path to the transaction JSON file

        Raises:
            MonzoExceptionError upon failure to upload transactions

        """

        import pandas as pd
        from monzo_transactions.upload_transactions import UploadTransactions

        with open(transactions_path, "rb") as transactions:
            final_transactions = loads(transactions.read())

        final_transactions_df = pd.DataFrame(final_transactions)

        final_transactions_df["date"] = pd.to_datetime(
            final_transactions_df["date"], unit="ms"
        )

        upload = UploadTransactions(
            transactions_df=final_transactions_df
        )

        new_transactions = upload.get_new_transactions()
        if len(new_transactions) > 0:
            try:
                upload.upload_new_transactions()
            except:
                raise MonzoException(
                    "An error occured while uploading new transactions"
                )

    monzo_api_authentication = get_monzo_auth_history()
    pull_transactions = get_monzo_transactions_history(monzo_api_authentication)
    upload_monzo_transactions_history(pull_transactions)


dag = monzo_transactions_history()
