#!/bin/python3

import sys
import json
import urllib.request
import logging.handlers
import datetime
import calendar
import csv
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
import re
import argparse
import os.path
import time
import shutil

#固定変数
##エクスポートファイル出力フォルダ
EXPORT_DIR = "./export/"
##データ取得プロセス数
PROCCESS_WORKER = 4
##ログローテーション設定
MAX_BYTES = 100 * 1024
LOTATE_COUNT = 4


#ログ出力の設定
global logger 
logger = logging.getLogger(__name__)
###debugレベルでのログ出力
logger.setLevel(logging.INFO)
##出力先を定義
###標準出力
stream_handler = logging.StreamHandler()
###ファイル出力
file_handler = logging.handlers.RotatingFileHandler(
    filename="zabbix_api.log",
    maxBytes=MAX_BYTES,
    backupCount=LOTATE_COUNT,
    encoding="utf-8")
###出力フォーマットの定義
formatter = logging.Formatter('%(asctime)s %(name)s %(funcName)s [%(levelname)s]: %(message)s')
stream_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)
##ログ出力の登録
logger.addHandler(stream_handler)
logger.addHandler(file_handler)



class Zabbix_Api:

    REQUEST_HEADER = {"Content-Type": "application/json-rpc"}

    api_url = ""
    token = ""
    last_request = None
    last_response_dict = None
    last_response = None

    def __init__(self, server="http://localhost", auth_token=""):
        """ 
            初期化 apiを利用するためのURLや認証トークンを利用する。

            Args:
                server (string): サーバのURL ex)http://localhost
                auth_token (string): 認証トークン 事前に取得しておく
        """
        #self.api_url = server + '/zabbix/api_jsonrpc.php'
        self.api_url = server + '/api_jsonrpc.php'
        self.token = auth_token

    def send_request(self, request_body):
        """ 
            指定されたJSONリクエストでAPIメソッドを呼び出して、レスポンスから戻り値を取り出して返す。

            Args:
                request_body (JSON): リクエストパラメータを含むJSONリクエスト

            Returns:
                JSONレスポンスから 'result' キーで取り出した何か
                型は各APIメソッドの定義に依存する
        """
        #リクエスト内容のログ出力
        logger.debug("request_body: {0}".format(request_body))

        # サーバのURL、指定されたJSONリクエスト、ヘッダでPOSTリクエストを生成する
        self.last_request = urllib.request.Request(
            url=self.api_url,
            data=json.dumps(request_body).encode(),
            headers=self.REQUEST_HEADER)
        
        def send():
            try:
                # APIを呼び出して生のレスポンスを保存する。
                self.last_response = urllib.request.urlopen(self.last_request)
                # 生レスポンスを read() で文字列型にしてから json.loads() で辞書型に変換する。
                self.last_response_dict =  json.loads(self.last_response.read())
                logger.debug("response_dict: {0}".format(self.last_response_dict))
                # 変換後の辞書オブジェクトから 'result' キーで戻り値を取り出して返す。
                return self.last_response_dict["result"]
            except Exception as e:
                logger.error('[0001] リクエスト送信エラー')
                logger.error("error message: {0}".format(e.args))
                raise e
        
        try:
            return send()
        except Exception as e:
            #リトライ
            send()


    def get_host_list(self):
        """ 
            ホスト一覧を取得する。

            Args:
                なし

            Returns:
                zabbix serverに登録しているホスト一覧をdictで返却
        """
        #全ホストを取得するリクエスト
        request_body = {
            "jsonrpc": "2.0",
            "method": "host.get",
            "params": {
                "output": ["hostid","name","host"],
                "sortfield": ["name"],
                "sortorder": ["ASC"],
            },
            "auth": self.token,
            "id": 1
        }
        #リクエストを送信
        try:
            return self.send_request(request_body)
        except Exception as e:
            #エラーログの出力
            logger.error('[9999] 不明なエラー')
            logger.error("error message: {0}".format(e.args))
            raise e


    def get_item_list(self,hostid):
        """ 
            数値データのアイテム一覧を取得する。

            Args:
                hostid:hostid

            Returns:
                hostidのアイテム一覧をdictで返却
        """
        #hostidの前item情報を取得するリクエスト
        request_body = {
            "jsonrpc": "2.0",
            "method": "item.get",
            "params": {
                "output": ["itemid","name","value_type"],
                "sortfield": ["name"],
                "sortorder": ["ASC"],
                "filter": {
                    "hostid": hostid,
                    "value_type":[0,3]
                }
            },
            "auth": self.token,
            "id": 1
        }
        #リクエストを送信
        try:
            return self.send_request(request_body)
        except Exception as e:
            #エラーログの出力
            logger.error('[9999] 不明なエラー')
            logger.error("error message: {0}".format(e.args))


    def get_history_itemid(self,hostid,itemid,item_type,start_time,end_time):
        """ 
            ヒストリ-を取得する。

            Args:
                itemid:itemid
                hostid:hostid
                start_time:取得したいデータの開始時間
                end_time:取得したデータの終了時間

            Returns:
                itemidのヒストリーデータをdictで返却
        """
        #ヒストリーデータを取得するリクエスト
        request_body = {
            "jsonrpc": "2.0",
            "method": "history.get",
            "params": {
                "output": "extend",
                "sortfield": ["clock"],
                "sortorder": ["ASC"],
                "history": item_type,
                "hostids": hostid,
                "itemids": itemid,
                "time_from": int(datetime.datetime.timestamp(start_time)),
                "time_till": int(datetime.datetime.timestamp(end_time))
            },
            "auth": self.token,
            "id": 1
        }

        #リクエストを送信
        try:
            responce_data =  self.send_request(request_body)
        except Exception as e:
            #エラーログの出力
            logger.error('[9999] 不明なエラー')
            logger.error("error message: {0}".format(e.args))
            return None
        else:
            #正常にデータが取得した場合
            #unix時間を時刻表現に変更
            for data in responce_data:
                data["clock"] = self.convert_time(int(data["clock"]))

            return responce_data
    
    def convert_time(self, unix_t, tz=9):
        """ 
        unix時間をtzで指定したタイムゾーンの時刻表記に変換

        Args:
            unix_t:変換したいunix時間
            tz:UTCからの時間差（defaultは9時間(JST)）

        Returns:
            変換した時刻の文字列
        """
        tz_jst = datetime.timezone(datetime.timedelta(hours=tz))
        dt = datetime.datetime.fromtimestamp(unix_t,tz_jst)
        return dt.strftime('%Y/%m/%d-%H:%M:%S')
        

def file_name_escape(str):
    """
        ファイル名に利用できない文字列、空白を'_' に変更する。
            \ / : ? . " < > 空白 ,

        Args:
            str:エスケープしたい文字列

        Returns:
            エスケープされた文字列を返却
    """
    return re.sub(r'\\|/|:|\?|\.|"|<|>|\s', '_', str)

def get_history_host(host, datetime_start, datetime_end, export_dir):
    """
        指定されたホストのヒストリーデータを取得する。

        Args:
            host:出力ホストの情報
            datetime_start:取得開始時間
            datetime_end:取得終了時間
            export_dir:ヒストリーデータ出力先ディレクトリ

        Returns:
            エスケープされた文字列を返却
    """
    #処理開始ログの出力
    logger.info("{0}のデータ取得開始".format(host["name"]))

    #zbxライブラリの初期化
    zbx = Zabbix_Api(server="http://172.16.144.164",auth_token="0f7467218cee8b119806b373c19993185df358b64beda4341451941cebfbaf7c")

    #出力先の作成
    try:
        #ファイル名に利用できない文字列を"_"に変換
        export_dir_host = os.path.join(export_dir, file_name_escape(host["name"]))
        os.makedirs(export_dir_host, exist_ok=True)
    except Exception as e:
        #エラーログの出力
        logger.error('[0002] "{0}"フォルダが作成できません。'.format(export_dir_host))
        logger.error("error message: {0}".format(e.args))
        return False

    #ホストに設定されている全アイテムの一覧を取得
    item_list = zbx.get_item_list(hostid=host["hostid"])

    for item in item_list:
        history_data = zbx.get_history_itemid(
            hostid=host["hostid"],
            itemid=item["itemid"],
            item_type=item["value_type"],
            start_time=datetime_start,
            end_time=datetime_end
        )
        #リクエストデータのログ出力
        logger.debug(history_data)

        #ファイル名に利用できない文字列を"_"に変換
        file_name = os.path.join(export_dir_host, file_name_escape(item["name"]) + ".csv")
        #ファイルへ書き込み
        try:
            with open(file_name, "w", newline="")as f:
                #ヘッダーの定義
                fieldnames = ["itemid", "clock", "value", "ns"]
                #ファイル出力
                dict_writer = csv.DictWriter(f, fieldnames=fieldnames)
                dict_writer.writeheader()
                dict_writer.writerows(history_data)
        except Exception as e:
            #エラーログの出力
            logger.error('[0003] {0}のヒストリーデータcsvが作成できません。'.format(item["name"]))
            logger.error("error message: {0}".format(e.args))
            return False

    #処理終了ログの出力
    logger.info("{0}のデータ取得終了".format(host["name"]))

    return True



def main():
    """
        main関数

        Args:
            なし
        
        Returns:
            なし
    """

    #実行開始時間取得
    start = time.time()

    logger.info("プログラムを開始します。")

    #コマンドライン引数の取得
    psr  = argparse.ArgumentParser(
        prog='zabbix history export tool',
        usage='',
        description=''
    )

    psr.add_argument('-ho', '--host', help='取得したいホスト名 入力がなければ全ホストを取得')
    psr.add_argument('-t', '--period', help='出力期間 td:当日分 ld:前日 tm:当月 ld:前月', default="td")
    psr.add_argument('-i', '--item', help='取得したいアイテム名 入力がなければ全アイテムを取得')

    args = psr.parse_args()


    #APIクラスの初期化
    zbx = Zabbix_Api(server="http://172.16.144.164",auth_token="0f7467218cee8b119806b373c19993185df358b64beda4341451941cebfbaf7c")


    #データ取得期間の設定
    ##当日取得
    if args.period == "td":
        #現在時刻の取得
        datetime_output = datetime.datetime.now()
        ##当日始の取得
        datetime_start = datetime.datetime(datetime_output.year, datetime_output.month, datetime_output.day, 0, 0, 0)
        ##当日終の取得
        datetime_end = datetime.datetime(datetime_output.year, datetime_output.month, datetime_output.day, 23, 59, 59)
        ##出力フォルダ
        export_dir = os.path.join(EXPORT_DIR, datetime_output.strftime("%Y%m%d"))
        ##出力ファイル名
        export_file = datetime_output.strftime("%Y%m%d")
    ##前日取得
    elif args.period == "ld":
        #昨日の取得
        datetime_output = datetime.datetime.now() - datetime.timedelta(days=1)
        ##昨日始の取得
        datetime_start = datetime.datetime(datetime_output.year, datetime_output.month, 1, 0, 0, 0)
        ##昨日末の取得
        datetime_end = datetime.datetime(datetime_output.year, datetime_output.month, datetime_output.day, 23, 59, 59)
        ##出力フォルダ
        export_dir = os.path.join(EXPORT_DIR, datetime_output.strftime("%Y%m%d"))
        ##出力ファイル名
        export_file = datetime_output.strftime("%Y%m%d")
    ##当月取得
    elif args.period == "tm":
        #現在時刻の取得
        datetime_output = datetime.datetime.now()
        ##月初の取得
        datetime_start = datetime.datetime(datetime_output.year, datetime_output.month, 1, 0, 0, 0)
        ##月末の取得
        datetime_end = datetime.datetime(datetime_output.year, datetime_output.month, calendar.monthrange(datetime_output.year, datetime_output.month)[1], 23, 59, 59)
        ##出力フォルダ
        export_dir = os.path.join(EXPORT_DIR, datetime_output.strftime("%Y%m"))
        ##出力ファイル名
        export_file = datetime_output.strftime("%Y%m")
    ##前月取得
    elif args.period == "lm":
        #先月の取得
        datetime_now = datetime.datetime.now()
        datetime_output = datetime.datetime(datetime_now.year, datetime_now.month, 1, 0, 0, 0) - datetime.timedelta(days=1)
        ##先月初の取得
        datetime_start = datetime.datetime(datetime_output.year, datetime_output.month, 1, 0, 0, 0)
        ##先月末の取得
        datetime_end = datetime.datetime(datetime_output.year, datetime_output.month, calendar.monthrange(datetime_output.year, datetime_output.month)[1], 23, 59, 59)
        ##出力フォルダ
        export_dir = os.path.join(EXPORT_DIR, datetime_output.strftime("%Y%m"))
        ##出力ファイル名
        export_file = datetime_output.strftime("%Y%m")
    else:
        logger.error('[0005] 引数が不正です。')
        logger.error('プログラムを異常終了します。')
        sys.exit(-1)


    try:
        #ホスト一覧を取得
        host_list = zbx.get_host_list()
        logger.debug(json.dumps(host_list,indent=2))
    except Exception as e:
        logger.error('[0006]Zabbix Serverからホスト一覧を取得できませんでした。')
        logger.error('プログラムを異常終了します。')
        sys.exit(-1)

    #出力先の作成
    try:
        os.makedirs(export_dir, exist_ok=True)
    except Exception as e:
        #エラーログの出力
        logger.error('[0002] "{0}"フォルダが作成できません。'.format(export_dir))
        logger.error('プログラムを異常終了します。')
        sys.exit(-1)

    if args.host is not None:
        #指定されたホストのみ取得
        ##ホスト名がZabbix Serverに登録されているかチェック
        host = [data for data in host_list if data["name"] == args.host]
        if not host:
            #ホスト名がZabbix Serverに登録されていない場合
            logger.error('[0007] "{0}"はZabbix Serverに登録されていません。'.format(args.host))
        else:
            #ホスト名がZabbix Serverに登録されている場合
            get_history_host(host[0], datetime_start, datetime_end,export_dir)

    else:
        #全ホストを取得(マルチプロセス)   
        with ProcessPoolExecutor(max_workers=PROCCESS_WORKER) as ppe:
            futures = [ppe.submit(get_history_host, host, datetime_start, datetime_end, export_dir) for host in host_list]
            #実行結果の表示
            for future in as_completed(futures):
                logger.info("実行結果:{0}".format(future.result()))

    try:
        logger.info("圧縮処理開始")
        #カレントディレクトリの取得
        before_dir = os.getcwd()
        #出力先に移動
        os.chdir(EXPORT_DIR)
    
        if args.host is not None:
            #指定されたホストのみ取得
            shutil.make_archive("{0}_{1}".format(export_file,file_name_escape(args.host)), format="zip", base_dir=export_file)
        else:
            #全ホストを取得
            shutil.make_archive(export_file, format="zip", base_dir=export_file)
        #元のディレクトリに戻る
        os.chdir(before_dir)
    except Exception as e:
        #エラーログの出力
        logger.error('[0008] ファイルの圧縮ができません。')
        logger.error('プログラムを異常終了します。')
        sys.exit(-1)
    else:
        logger.info("圧縮処理終了")
        #圧縮が成功した場合はディレクトリを削除する
        shutil.rmtree(export_dir)


    #実行時間の記録
    logger.info("実行時間:{0:.3f}秒".format(time.time() - start))
    logger.info("プログラムを正常終了します。")

if __name__ == '__main__':
    #main関数の実行
    main()
