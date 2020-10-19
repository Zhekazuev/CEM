from clickhouse_driver import connect
import pandas as pd
import config
import sys


def logic(df):
    pd.options.display.max_rows = 150
    # statistics
    statistics = (df.groupby(['SN CHARGING ACTION', 'P2P PROTOCOL'])[["BYTES DOWNLINK", "BYTES UPLINK"]]
                  .sum().div(1024 ** 2))
    statistics["UPLINK PLUS DOWNLINK"] = statistics["BYTES DOWNLINK"] + statistics["BYTES UPLINK"]
    statistics = (statistics.sort_values('UPLINK PLUS DOWNLINK', ascending=False)
                  .sort_index(level=0, sort_remaining=False))
    print("Total statistics:\n", statistics, end="\n\n\n")

    statistics_by_actions = df.groupby(['SN CHARGING ACTION'])[["BYTES DOWNLINK", "BYTES UPLINK"]].sum().div(1024 ** 2)
    statistics_by_actions["UPLINK PLUS DOWNLINK"] = (statistics_by_actions["BYTES DOWNLINK"] +
                                                     statistics_by_actions["BYTES UPLINK"])
    print("Total statistics group by charging actions:\n", statistics_by_actions)
    total_uplink_plus_downlink = statistics_by_actions["UPLINK PLUS DOWNLINK"].sum()
    print("Total usage traffic:", total_uplink_plus_downlink, end="\n\n\n")

    statistics_by_ips = df.groupby(['SN CHARGING ACTION', 'P2P PROTOCOL', 'SERVER IP ADDRESS'])[
        ["BYTES DOWNLINK", "BYTES UPLINK"]].sum().div(1024 ** 2)
    statistics_by_ips["UPLINK PLUS DOWNLINK"] = (statistics_by_ips["BYTES DOWNLINK"] +
                                                 statistics_by_ips["BYTES UPLINK"])

    statistics_by_ips = statistics_by_ips[statistics_by_ips["UPLINK PLUS DOWNLINK"] > 50] \
        .nlargest(50, columns='UPLINK PLUS DOWNLINK') \
        .groupby(['SN CHARGING ACTION', 'P2P PROTOCOL', 'SERVER IP ADDRESS']).sum() \
        .sort_values(['SN CHARGING ACTION', 'P2P PROTOCOL', 'UPLINK PLUS DOWNLINK'], ascending=False) \
        .sort_index(level=0, sort_remaining=False)
    print("Top statistics group by charging action, protocol and IP addresses:\n", statistics_by_ips, end="\n\n\n")

    # get all unique protocols
    protocols = df.get('P2P PROTOCOL').dropna().unique().tolist()
    # get all unique addresses
    addresses = df.groupby('P2P PROTOCOL')['SERVER IP ADDRESS'].unique()

    # telegram
    no_messengers = df[df['SN CHARGING ACTION'] == 'charge-10000']
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
    try:
        instagram_addresses = addresses["instagram"]
        df_1 = df[df['SN CHARGING ACTION'] == 'charge-10000']
        df_2 = df_1[df_1['SERVER IP ADDRESS'].isin(instagram_addresses)]
        total_instagram_traffic = ((df_2.groupby('SN CHARGING ACTION')['BYTES DOWNLINK'].sum() +
                                    df_2.groupby('SN CHARGING ACTION')['BYTES UPLINK'].sum())
                                   .sort_values(ascending=False).sum() / 1024 ** 2)
        print(f"Total Instagram traffic for return to subscriber: {total_instagram_traffic} MB")
    except KeyError:
        total_instagram_traffic = 0

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
    # check data finish
    data_finish = input("Input Data finish here:")

    con = connect(host=config.host,
                  port=config.port,
                  user=config.login,
                  password=config.password)

    query = f"""SELECT RADIUS_ID, START_TIME, END_TIME, RULEBASE, BYTES_UPLINK, BYTES_DOWNLINK, 
            SN_CHARGING_ACTION, P2P_PROTOCOL, SERVER_IP_ADDRESS, P2P_TLS_SNI, P2P_TLS_CNAME
            FROM DPI.FLOW_SHARDED
            PREWHERE RADIUS_ID = {number}
            WHERE END_TIME >= toDateTime('{data_start} 00:00:00') and 
            END_TIME < toDateTime('{data_finish} 23:59:59')"""
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
