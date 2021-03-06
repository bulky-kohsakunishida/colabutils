import os
import zipfile
from google.cloud import storage
from google.colab import auth
from datetime import datetime, timedelta, timezone

from pathlib import Path
from typing import Iterable, List
import subprocess


class GcsUtils:

    def __init__(self, project_id: str, bucket_name: str, work_name: str, result_path: str, weight_path: str) -> None:
        """
        <init>
        Args:
            project_id str:
                Google Cloud Storageのプロジェクト名
            bucket_name str:
                Google Cloud Storageのバケット名
            work_name str:
                検出対象の製品名
            result_path str:
                Google Cloud Storageの結果を保存する場所のパス
            weight_path str:
                Google Cloud Storageの重みを保存する場所のパス
        """
        auth.authenticate_user()
        self.storage_client = storage.Client(project=project_id)
        self.bucket = self.storage_client.get_bucket(bucket_name)
        self._work_name = work_name
        self._result_path = result_path
        self._weight_path = weight_path

    def download(self, gcs_dl_path: str, local_name: str, exist_ok=False, unzip_flag=False) -> None:
        """
        <download>
            GCSの指定したパスから指定したファイルをローカルにダウンロードする。
        Args:
            gcs_dl_path str:
                Google Cloud Storageのダウンロードパス
            local_name str:
                ローカルでのファイル名
            exist_ok boolean:
                同じファイル名が存在する場合上書きを行うか、False=重複禁止、True=上書き
            unzip_flag boolean:
                zipファイルを解凍するか、False=解凍しない、zipファイルではない、True=zipファイルを解凍する
        """
        if not os.path.exists(local_name) or exist_ok:
            blob = self.bucket.blob(gcs_dl_path)
            blob.download_to_filename(local_name)
            print(f"{local_name}をダウンロードしました")
            if unzip_flag:
                with zipfile.ZipFile(local_name) as zf:
                    zf.extractall()

    def upload(self, local_name: str, gcs_ul_path: str):
        """
        <upload>
            指定したファイルをGCSの指定したパスにアップロードする。
        Args:
            local_name str:
                ローカルでのファイル名
            gcs_ul_path str:
                Google Cloud Storageのアップロードパス
        """
        blob = self.bucket.blob(gcs_ul_path)
        blob.upload_from_filename(local_name)
        print(f"{local_name}をアップロードしました")

    def zip_upload(self, local_name: str, gcs_ul_path: str, zip_name: str):
        """
        <zip_upload>
            指定したファイルを圧縮した後にGCSの指定したパスにアップロードする。
        Args:
            local_name str:
                ローカルでのファイル名
            gcs_ul_path str:
                Google Cloud Storageのアップロードパス
            zip_name str:
                圧縮したあとのファイル名
        """
        zf = zipfile.ZipFile(zip_name + ".zip", 'w')
        zf.write(local_name, zip_name, compress_type=zipfile.ZIP_DEFLATED)
        zf.close()
        self.upload(zip_name + ".zip", gcs_ul_path)

    def output_upload(self, result_dir, model_name, check_point_file, learning_rate, max_itr):
        """
        <output_upload>
            学習結果をGCSの指定したパスにアップロードする。
        Args:
            result_dir str:
                学習結果が格納されたローカルのディレクトリ
            model_name str:
                最終保存された重みファイル
            check_point_file str:
                途中保存された重みファイル
            learning_rate double:
                学習率
            max_itr int:
                学習回数
        """
        if os.path.exists(check_point_file):
            os.remove(check_point_file)

        _results_file = self.__train_prepare_upload(learning_rate, max_itr)

        # 重みファイル
        self._weight_name = _results_file + '.pth'

        # outputフォルダのzip
        self._result_file_zip = _results_file + '.zip'

        if os.path.exists(model_name):
            self.upload(model_name, os.path.join(self.weight_path, self._weight_name))

        self.__compress_dir(Path(self._result_file_zip), Path(result_dir), ".pth")

        self.upload(self._result_file_zip, os.path.join(self.result_path, self._result_file_zip))

    def __train_prepare_upload(self, learning_rate, max_itr):
        """
        <__train_prepare_upload>
            学習結果のアップロードする際に命名する文字列を生成し、返す。
        Args:
            learning_rate double:
                学習率
            max_itr int:
                イテレーション数
        Return:
            results_file str:
                日付、時間、ワーク名、学習率、学習回数を組み合わせた文字列
        """
        # タイムゾーンの生成
        JST = timezone(timedelta(hours=+9), 'JST')
        tdatetime = datetime.now(JST)
        date_stirngs = tdatetime.strftime('%Y%m%d')
        time_strings = tdatetime.strftime('%H%M')
        results_file = date_stirngs + '_' + time_strings + '_' + "results_train" + '_' + self.work_name + '_' + f"{learning_rate:.0e}" + '_' + str(max_itr)

        return results_file

    def inference_upload(self, result_dir, thresh_hold):
        """
        <inference_upload>
            推論結果をGCSの指定したパスにアップロードする。
        Args:
            result_dir str:
                推論結果が格納されたローカルのディレクトリ
            thresh_hold double:
                異常の閾値
        """
        _results_file = self.__inference_prepare_upload(thresh_hold)

        # predict_images フォルダのzip
        self._result_file_zip = _results_file + '.zip'

        self.zip_upload(result_dir, os.path.join(self.result_path, self._result_file_zip), _results_file)

    def __inference_prepare_upload(self, thresh_hold):
        """
        <__inference_prepare_upload>
            学習結果のアップロードする際に命名する文字列を生成し、返す。
        Args:
            thresh_hold double:
                異常の閾値
        Return:
            results_file str:
                日付、時間、ワーク名、閾値を組み合わせた文字列
        """
        # タイムゾーンの生成
        JST = timezone(timedelta(hours=+9), 'JST')
        tdatetime = datetime.now(JST)
        date_stirngs = tdatetime.strftime('%Y%m%d')
        time_strings = tdatetime.strftime('%H%M')
        results_file = date_stirngs + '_' + time_strings + '_' + "results_inference" + '_' + self.work_name + '_th' + str(thresh_hold)

        return results_file

    def __compress_dir(self, zip_out: Path, directory: Path, skipped_names: Iterable[str]):
        """
        <__compress_dir>
            指定されたファイルをzip形式で圧縮する。
        Args:
            zip_out Path:
                圧縮後の名前を含むパス
            directory Path:
                圧縮対象ディレクトリ
            skipped_name Iterable[str]:
                圧縮対象ディレクトリ内のファイルから指定した文字列を含むものを除いて圧縮
        """
        # ファイル圧縮
        settings = {
            'verbose': False,
        }
        with zipfile.ZipFile(zip_out.absolute(), 'w') as z:
            if settings['verbose']:
                print('Zip file has been initialized: "{}".'.format(z.filename))
            for f in self.__iter_all_files(directory):
                # 特定の文字列を含むファイルを除く
                if skipped_names in f.name:
                    if settings['verbose']:
                        print('Skipped: "{}".'.format(f))
                    continue

                z.write(f)
                if settings['verbose']:
                    print('Added: "{}".'.format(f))

    def __iter_all_files(self, directory: Path) -> List[Path]:
        """
        <__iter_all_files>
            指定されたディレクトリの下にあるすべてのファイル（ Path ）を下層のものも含めて返すイテレータを返す。
        Args:
            directory Path:
                検索対象のディレクトリ
        Return:
            directory.glob('**/*.*')
                ディレクトリ内のすべてのファイル
        """
  
        if not directory.is_dir():
            message = 'Specified directory is not found: "{}".'.format(directory)
            raise self.CompressionError(message)

        return directory.glob('**/*.*')

    class CompressionError(Exception):
        """
        zip 圧縮プロセスで発生するエラーを表すカスタム例外
        """
        pass

    @property
    def work_name(self):
        return self._work_name

    @property
    def result_path(self):
        return self._result_path

    @property
    def weight_path(self):
        return self._weight_path


def download_keys(gcsutils: GcsUtils) -> None:
    """
    tor key file
    :param gcsutils: GcsUtils instance
    :return: None
    """
    keyfile = 'bulkyadkeys.zip'
    gcsutils.download(keyfile, keyfile, exist_ok=True, unzip_flag=True)

    subprocess.run('mkdir -p /root/.ssh', shell=True,
                   stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    subprocess.run('mv id_rsa /root/.ssh', shell=True,
                   stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    subprocess.run('ssh-keyscan -t rsa github.com >> ~/.ssh/known_hosts', shell=True,
                   stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    subprocess.run('chmod go-rwx /root/.ssh/id_rsa', shell=True,
                   stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
