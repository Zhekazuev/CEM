import pandas as pd
import config
import sys


def file(path):
    df = pd.read_csv(path, sep=";")

    # protocol statistics
    protocol_statisctics = ((df.groupby('P2P PROTOCOL')['BYTES DOWNLINK'].sum() / (1024 ** 2) +
                             df.groupby('P2P PROTOCOL')['BYTES UPLINK'].sum() / (1024 ** 2))
                            .sort_values(ascending=False))
    print("Summury protocols statistics:", protocol_statisctics)

    # charging action statistics
    actions_statisctics = ((df.groupby('SN CHARGING ACTION')['BYTES DOWNLINK'].sum() / (1024 ** 2) +
                            df.groupby('SN CHARGING ACTION')['BYTES UPLINK'].sum() / (1024 ** 2))
                           .sort_values(ascending=False))
    print("Summury charging actions statistics:", actions_statisctics)

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
                      (df_ipstarts_451['BYTES UPLINK'].sum())).sort_values(ascending=False).sum()/(1024**2)
    traffic_ip_1965 = ((df_ipstarts_1965['BYTES DOWNLINK'].sum()) +
                       (df_ipstarts_1965['BYTES UPLINK'].sum())).sort_values(ascending=False).sum()/(1024**2)
    traffic_ip_1491 = ((df_ipstarts_1491['BYTES DOWNLINK'].sum()) +
                       (df_ipstarts_1491['BYTES UPLINK'].sum())).sort_values(ascending=False).sum()/(1024**2)
    summary_telegram_traffic = traffic_ip_451 + traffic_ip_1965 + traffic_ip_1491
    print(f"Summary telegram traffic for return: {summary_telegram_traffic} MB")

    # instagram
    instagram_addresses = addresses["instagram"]
    df_1 = df[df['SN CHARGING ACTION'] != 'Social-net']
    df_2 = df_1[df_1['SERVER IP ADDRESS'].isin(instagram_addresses)]
    summary_instagram_traffic = ((df_2.groupby('SN CHARGING ACTION')['BYTES DOWNLINK']).sum() +
                                 (df_2.groupby('SN CHARGING ACTION')['BYTES UPLINK']).sum()) / (1024 ** 2)
    print(f"Summary instagram traffic for return: {summary_instagram_traffic} MB")

    # youtube

    return f"Summury traffic for return: {summary_telegram_traffic + summary_instagram_traffic} MB"


def database():
    pass


def main():
    try:
        path = sys.argv[1]
        print(file(path))
    except IndexError:
        print(database())


if __name__ == '__main__':
    main()
