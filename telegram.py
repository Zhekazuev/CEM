import pandas as pd
import config
import sys


def file(path):
    df = pd.read_csv(path, sep=";")

    protocol_statisctics = ((df.groupby('P2P PROTOCOL')['BYTES DOWNLINK'].sum() / (1024 ** 2) +
                             df.groupby('P2P PROTOCOL')['BYTES UPLINK'].sum() / (1024 ** 2))
                            .sort_values(ascending=False))
    print("Summury protocols statistics:", protocol_statisctics)

    actions_statisctics = ((df.groupby('SN CHARGING ACTION')['BYTES DOWNLINK'].sum() / (1024 ** 2) +
                            df.groupby('SN CHARGING ACTION')['BYTES UPLINK'].sum() / (1024 ** 2))
                           .sort_values(ascending=False))
    print("Summury charging actions statistics:", actions_statisctics)

    df_no_messengers = df[df['SN CHARGING ACTION'] != 'Messengers']
    df_ipstarts_451 = (df_no_messengers[df_no_messengers['SERVER IP ADDRESS'].str.match('^45.1*') == True]
                       .groupby('SN CHARGING ACTION'))
    df_ipstarts_1965 = (df_no_messengers[df_no_messengers['SERVER IP ADDRESS'].str.match('^196.5*') == True]
                        .groupby('SN CHARGING ACTION'))
    df_ipstarts_1491 = (df_no_messengers[df_no_messengers['SERVER IP ADDRESS'].str.match('^149.1*') == True]
                        .groupby('SN CHARGING ACTION'))

    traffic_ip_451 = ((df_ipstarts_451['BYTES DOWNLINK'].sum()) +
                      (df_ipstarts_451['BYTES UPLINK'].sum())).sort_values(ascending=False).sum()/(1024**2)
    traffic_ip_1965 = ((df_ipstarts_1965['BYTES DOWNLINK'].sum()) +
                       (df_ipstarts_1965['BYTES UPLINK'].sum())).sort_values(ascending=False).sum()/(1024**2)
    traffic_ip_1491 = ((df_ipstarts_1491['BYTES DOWNLINK'].sum()) +
                       (df_ipstarts_1491['BYTES UPLINK'].sum())).sort_values(ascending=False).sum()/(1024**2)

    return f"Summury traffic for return: {traffic_ip_451 + traffic_ip_1965 + traffic_ip_1491} MB"


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
