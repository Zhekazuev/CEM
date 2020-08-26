from clickhouse_driver import connect
import pandas as pd
import config
import sys


def logic(df):
    # protocol statistics
    sn_charging_action_by_protocol = df.groupby('P2P PROTOCOL')[["BYTES DOWNLINK", "BYTES UPLINK"]].sum().div(1024 ** 2)
    sn_charging_action_by_protocol['UPLINK PLUS DOWNLINK'] = (
            df.groupby('P2P PROTOCOL')['BYTES DOWNLINK'].sum().div(1024 ** 2) +
            df.groupby('P2P PROTOCOL')['BYTES UPLINK'].sum().div(1024 ** 2))
    print("Total statistics by Protocols:", sn_charging_action_by_protocol.sort_values(by='UPLINK PLUS DOWNLINK',
                                                                                       ascending=False))

    # charging action statistics
    sn_charging_action_by_rules = (df.groupby('SN CHARGING ACTION')[["BYTES DOWNLINK", "BYTES UPLINK"]].sum()
                                   .div(1024 ** 2))
    sn_charging_action_by_rules['UPLINK PLUS DOWNLINK'] = (
            df.groupby('SN CHARGING ACTION')['BYTES DOWNLINK'].sum().div(1024 ** 2) +
            df.groupby('SN CHARGING ACTION')['BYTES UPLINK'].sum().div(1024 ** 2))
    print("Total statistics by Rules:", sn_charging_action_by_rules.sort_values(by='UPLINK PLUS DOWNLINK',
                                                                                ascending=False))

    # get all unique protocols
    protocols = df.get('P2P PROTOCOL').dropna().unique().tolist()
    # get all unique addresses
    addresses = df.groupby('P2P PROTOCOL')['SERVER IP ADDRESS'].unique()

    # instagram
    instagram_addresses = addresses["instagram"]
    df_1 = df[df['SN CHARGING ACTION'] != 'Social-net']
    df_2 = df_1[df_1['SERVER IP ADDRESS'].isin(instagram_addresses)]
    total_instagram_traffic = ((df_2.groupby('SN CHARGING ACTION')['BYTES DOWNLINK'].sum() +
                               df_2.groupby('SN CHARGING ACTION')['BYTES UPLINK'].sum())
                               .sort_values(ascending=False).sum()["charge-10000"]/1024 ** 2)

    return f"Total instagram traffic for return to subscriber: {total_instagram_traffic} MB"


def database():
    print("Phone number format: 375291234567")
    # check number format
    number = input("Input MSISDN here:")

    print("Data start format: 21-08-2020")
    # check data start
    data_start = input("Input Data start here:")

    print("Data finish format: 21-08-2020")
    data_start = input("Input Data finish here:")

    con = connect(host=config.host,
                  port=config.port,
                  user=config.login,
                  password=config.password)

    query = ""
    df = pd.read_sql_query(query, con)

    return df


def main():
    try:
        path = sys.argv[1]
        df = pd.read_csv(path, sep=";")
    except IndexError:
        df = database()

    return logic(df)


if __name__ == '__main__':
    print(main())
