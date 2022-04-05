import os
from google.colab import auth

def GCS_download(PROJECT_ID, BUCKET_NAME, DATASET_PATH, DATASET_NAME):
    if not os.path.exists(DATASET_NAME + '.zip'):
        # 認証
        auth.authenticate_user()

        # Google Cloud Storage からデータセットと学習済モデルをコピーし、データセット展開する
        !gcloud config set project {PROJECT_ID}

        !gsutil cp gs://{BUCKET_NAME}/{DATASET_PATH}/{DATASET_NAME}.zip {DATASET_NAME}.zip
        !unzip {DATASET_NAME}.zip > /dev/null

def GCS_upload(WORK_NAME, BUCKET_NAME, WEIGHT_PATH):
    RESULTS_DIR = 'output'

    check_point_file = os.path.join(RESULTS_DIR, 'checkpoint.pth')

    # タイムゾーンの生成
    JST = timezone(timedelta(hours=+9), 'JST')

    # result file name exp. 2101012359_notebook_name_dataset001_1e-3_1000
    tdatetime = datetime.now(JST)
    date_stirngs = tdatetime.strftime('%y%m%d')
    time_strings = tdatetime.strftime('%H%M')
    results_file = datetime_stirngs + '_' + time_strings + '_' + "results_train" + '_' + WORK_NAME + '_' + f"{hparam_dict['LEARNING_RATE']:.0e}" + '_' + str(
        hparam_dict['MAX_ITR'])

    # outputフォルダのzip
    RESULTS_FILE_ZIP = results_file + '.zip'

    # 重みファイル
    WEIGHT_NAME = results_file + '.pth'

    if os.path.exists(check_point_file):
        os.remove(check_point_file)

    # 重みファイル(model_final.pthで出力)をリネームし、GCSに格納
    model_name = "output/model_final.pth"
    if os.path.exists(model_name):
        print('save weight file :gs://{}/{}/{}'.format(BUCKET_NAME, WEIGHT_PATH, WEIGHT_NAME))
        !gsutil cp {model_name} gs://{BUCKET_NAME} / {WEIGHT_PATH} / {WEIGHT_NAME}
