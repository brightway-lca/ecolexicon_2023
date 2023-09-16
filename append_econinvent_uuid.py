import pandas as pd


def main():
    # read excel
    ecoinvent_int_exchange_df = pd.read_excel('data/ecoinvt_int_exchange.xlsx')
    hestia_export_df = pd.read_excel('data/transport.xlsx')

    # find all terms in ecoinvent data
    ecoinvent_terms = ecoinvent_int_exchange_df[['name', 'uuid']]
    # make the column 'name' the index.
    ecoinvent_terms.set_index('name', inplace=True)
    # convert the dataframe to a dictionary with the key being the index
    ecoinvent_terms_dict = ecoinvent_terms.to_dict()['uuid']
    # add string column ecoinvent_uuid to hestia df
    hestia_export_df['ecoinvent_uuid'] = ''
    hestia_export_df['ecoinvent_name'] = ''

    # iterate hestia rows and lookt at 'lookups.0.value'
    for index, row in hestia_export_df.iterrows():
        orig_term = row['lookups.0.value']
        # order keys alphabetically and with longest first
        keys_sorted = sorted(ecoinvent_terms_dict.keys(), key=len, reverse=True)
        # if there is a match, append the uuid to the row
        for term in keys_sorted:
            if pd.isna(term):
                continue
            # does the original term start with term?
            if str(orig_term).startswith(term):
                hestia_export_df.at[index, 'ecoinvent_uuid'] = ecoinvent_terms_dict[str(term)]
                hestia_export_df.at[index, 'ecoinvent_name'] = term
                break
    # export to excel
    hestia_export_df.to_excel('data/transport_uuid.xlsx')


if __name__ == '__main__':
    main()
