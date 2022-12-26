import numpy as np
import notion_df as ndf
import os
import logging


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class UploadTransactions:
    """Class to manage uploading transactions to a Notion database"""

    def __init__(
        self,
        transactions_df
    ):
        """
        Initialise UploadTransactions

        Args:
            transactions: Dataframe of transactions pulled
            schema: Database schema name
            table: Database table name
            changed_transactions: Dataframe of changed transactions to upload
            transactions_to_upload: Dataframe of new transactions to upload
        """
        self.transactions_df = transactions_df

    def _get_db_transactions(self):
        """
        Private method to get existing transactions from database

        Returns:
            Dataframe of existing transactions in database
        """
        db_transactions = ndf.download(
            notion_url=os.getenv("NOTION_MONZO_TRANSACTIONS_DB_URL"), api_key=os.getenv("NOTION_API_KEY")
        )

        return db_transactions

    # def _delete(self, data):
    #     """
    #     Private method to delete transactions from the database

    #     Args:
    #         data: Dataframe of transactions that are to be deleted
    #     """
    #     sql_delete = sql_templates.delete.format(
    #         schema=self.schema, table=self.table, transactions=data
    #     )

    #     self.db.query(sql=sql_delete, return_data=False)
    #     logger.debug("Deleted rows for re-insertion")

    def _insert(self, data):
        """
        Private method to insert transactions into the database

        Args:
            data: Dataframe of transactions that are to be inserted
        """
        ndf.upload(
            df=data, notion_url=os.getenv("NOTION_MONZO_TRANSACTIONS_DB_URL"), api_key=os.getenv("NOTION_API_KEY")
        )

    def get_new_transactions(self):
        """
        Method to identify new transactions from pulled Monzo transactions

        Returns:
            Dataframe of new transactions
        """
        db_transactions = self._get_db_transactions()

        db_ids_lst = db_transactions["ID"].tolist()

        new_transaction_ids = []
        for item in self.transactions_df["ID"].tolist():
            if item not in db_ids_lst:
                new_transaction_ids.append(item)

        self.transactions_to_upload = self.transactions_df[
            self.transactions_df["ID"].isin(new_transaction_ids)
        ].reset_index(drop=True)

        return self.transactions_to_upload

    # def get_changed_transactions(self):
    #     """
    #     Method to identify changed transactions since last pull

    #     Returns:
    #         Dataframe of changed transactions
    #     """
    #     db_transactions = self._get_db_transactions()
    #     db_transactions_id_lst = db_transactions["id"].tolist()

    #     historical_transactions_in_request = self.transactions[
    #         self.transactions["id"].isin(db_transactions_id_lst)
    #     ]

    #     historical_transactions_in_request = historical_transactions_in_request[
    #         compare_transaction_cols
    #     ]
    #     historical_transactions_in_request = (
    #         historical_transactions_in_request.sort_values("id")
    #     )
    #     historical_transactions_in_request = (
    #         historical_transactions_in_request.reset_index(drop=True)
    #     )
    #     historical_transactions_in_request = historical_transactions_in_request.replace(
    #         {None: np.nan}
    #     )
    #     historical_transactions_in_request = historical_transactions_in_request.fillna(
    #         0
    #     )
    #     historical_transactions_in_request[
    #         "amount"
    #     ] = historical_transactions_in_request["amount"].astype(float)
    #     historical_transactions_in_request[
    #         "amount"
    #     ] = historical_transactions_in_request["amount"].round(2)

    #     historical_transactions_in_request_ids = historical_transactions_in_request[
    #         "id"
    #     ].tolist()

    #     historical_transactions = db_transactions[
    #         db_transactions["id"].isin(historical_transactions_in_request_ids)
    #     ]
    #     historical_transactions = historical_transactions.sort_values("id")
    #     historical_transactions = historical_transactions.reset_index(drop=True)
    #     historical_transactions = historical_transactions.replace({None: np.nan})
    #     historical_transactions = historical_transactions.fillna(0)
    #     historical_transactions["amount"] = historical_transactions["amount"].astype(
    #         float
    #     )
    #     historical_transactions["amount"] = historical_transactions["amount"].round(2)

    #     self.changed_transactions = historical_transactions_in_request[
    #         historical_transactions.ne(historical_transactions_in_request).any(axis=1)
    #     ]

    #     return self.changed_transactions

    def upload_new_transactions(self):
        """
        Method to upload new transactions
        """
        self._insert(self.transactions_to_upload)
        logger.debug("Uploaded new transactions")

    # def update_changed_transactions(self):
    #     """
    #     Method to update changed transactions
    #     """
    #     transactions_to_delete_ids = self.changed_transactions["id"].tolist()
    #     transactions_to_delete_ids_str = (
    #         str(self.changed_transactions["id"].tolist()).strip("[").strip("]")
    #     )

    #     self._delete(transactions_to_delete_ids_str)

    #     transactions_to_reinsert = self.transactions[
    #         self.transactions["id"].isin(transactions_to_delete_ids)
    #     ]

    #     self._insert(transactions_to_reinsert)
    #     logger.debug("Updated changed transactions")
