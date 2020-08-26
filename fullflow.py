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
    print("Total statistics by Protocols:\n", sn_charging_action_by_protocol.sort_values(by='UPLINK PLUS DOWNLINK',
                                                                                         ascending=False), end="\n\n\n")

    # charging action statistics
    sn_charging_action_by_rules = (df.groupby('SN CHARGING ACTION')[["BYTES DOWNLINK", "BYTES UPLINK"]].sum()
                                   .div(1024 ** 2))
    sn_charging_action_by_rules['UPLINK PLUS DOWNLINK'] = (
            df.groupby('SN CHARGING ACTION')['BYTES DOWNLINK'].sum().div(1024 ** 2) +
            df.groupby('SN CHARGING ACTION')['BYTES UPLINK'].sum().div(1024 ** 2))
    print("Total statistics by Rules:\n", sn_charging_action_by_rules.sort_values(by='UPLINK PLUS DOWNLINK',
                                                                                  ascending=False), end="\n\n\n")

    # get all unique protocols
    protocols = df.get('P2P PROTOCOL').dropna().unique().tolist()
    # get all unique addresses
    addresses = df.groupby('P2P PROTOCOL')['SERVER IP ADDRESS'].unique()

    # telegram
    no_messengers = df[df['SN CHARGING ACTION'] != 'Messengers']
    df_ipstarts_451 = (no_messengers[no_messengers['SERVER IP ADDRESS'].str.match('^45.1*') == True]
                       .groupby('SN CHARGING ACTION'))
    df_ipstarts_1965 = (no_messengers[no_messengers['SERVER IP ADDRESS'].str.match('^196.5*') == True]
                        .groupby('SN CHARGING ACTION'))
    df_ipstarts_1491 = (no_messengers[no_messengers['SERVER IP ADDRESS'].str.match('^149.1*') == True]
                        .groupby('SN CHARGING ACTION'))
    traffic_ip_451 = ((df_ipstarts_451['BYTES DOWNLINK'].sum()) +
                      (df_ipstarts_451['BYTES UPLINK'].sum())).sort_values(ascending=False).sum() / 1024 ** 2
    traffic_ip_1965 = ((df_ipstarts_1965['BYTES DOWNLINK'].sum()) +
                       (df_ipstarts_1965['BYTES UPLINK'].sum())).sort_values(ascending=False).sum() / 1024 ** 2
    traffic_ip_1491 = ((df_ipstarts_1491['BYTES DOWNLINK'].sum()) +
                       (df_ipstarts_1491['BYTES UPLINK'].sum())).sort_values(ascending=False).sum() / 1024 ** 2
    total_telegram_traffic = traffic_ip_451 + traffic_ip_1965 + traffic_ip_1491
    print(f"Total Telegram traffic for return to subscriber: {total_telegram_traffic} MB")

    # instagram
    instagram_addresses = addresses["instagram"]
    df_1 = df[df['SN CHARGING ACTION'] != 'Social-net']
    df_2 = df_1[df_1['SERVER IP ADDRESS'].isin(instagram_addresses)]
    total_instagram_traffic = ((df_2.groupby('SN CHARGING ACTION')['BYTES DOWNLINK'].sum() +
                                df_2.groupby('SN CHARGING ACTION')['BYTES UPLINK'].sum())
                               .sort_values(ascending=False).sum() / 1024 ** 2)
    print(f"Total Instagram traffic for return to subscriber: {total_instagram_traffic} MB")

    # youtube
    # summary_youtube_traffic =

    total_traffic = total_telegram_traffic + total_instagram_traffic
    return f"Total traffic for return to subscriber: {total_traffic} MB"


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
