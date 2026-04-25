import os
import sys
import json
from dotenv import load_dotenv
from atproto import Client
import sqlite3
import requests
from PIL import Image
import mimetypes
import logging

def main():
    """ For youフィードの投稿を100件取得し、投稿に含まれた当行と画像を保存する。 """
    """ 外部アクセスを禁止しているユーザーの投稿は取得しない。画像にポルノなどのR-18要素が含まれている投稿も保存しない。 """
    print("フィードの取り込み処理を開始します", flush=True)

    # .envファイルからアカウント情報を読み込む
    account = os.getenv("ACCOUNT")
    password = os.getenv("PASSWORD")

    if not account or not password:
        errmsg = f'.env ファイルに ACCOUNT または PASSWORD が設定されていません。'
        logging.error(errmsg)
        print(errmsg, file=sys.stderr)
        sys.exit(1)

    # Blueskyにログイン
    client = Client()
    try:
        client.login(account, password)
    except Exception as e:
        errmsg = f"ログインエラー: {e}"
        logging.error(errmsg)
        print(errmsg, file=sys.stderr)
        sys.exit(1)

    # デフォルトは「For You」フィードのURI
    feed_uri = os.getenv("FEED_URI", "at://did:plc:3guzzweuqraryl3rdkimjamk/app.bsky.feed.generator/for-you")

    try:
        print("フィードの読み込み中…", flush=True)
        # 先頭から上位100件の投稿を読み込む
        response = client.app.bsky.feed.get_feed({'feed': feed_uri, 'limit': 100})
    except Exception as e:
        errmsg = f"フィード取得エラー: {e}"
        logging.error(errmsg)
        print(errmsg, file=sys.stderr)
        sys.exit(1)

    results = []

    # 取得した投稿に対する条件フィルタリング
    for item in response.feed:
        try:
            post = item.post
            record = post.record

            # [条件] 投稿がリポストである (フィード上の理由がリポスト)
            if hasattr(item, 'reason') and item.reason:
                continue

            embed = getattr(record, 'embed', None)
            
            # [条件] 投稿に画像が1つも含まれていない (embedがない場合)
            if not embed:
                continue

            # Embedの型を取得 (SDKのバージョンによって属性名が異なる場合への安全対策)
            embed_type = getattr(embed, 'py_type', getattr(embed, '$type', ''))

            # [条件] 投稿が引用である (record または recordWithMedia)
            if 'record' in embed_type.lower():
                continue

            # [条件] 画像以外のメディア (動画など) である
            if embed_type != 'app.bsky.embed.images':
                continue

            # [条件] アプリがログアウトしたユーザーに自分のアカウントを表示しないオプション
            # アカウントの labels に '!no-unauthenticated' があるか確認
            skip_unauthenticated = False
            if hasattr(post.author, 'labels') and post.author.labels:
                for label in post.author.labels:
                    if label.val == '!no-unauthenticated':
                        skip_unauthenticated = True
                        break
            if skip_unauthenticated:
                continue

            # [条件] ポルノ、暴力、ゴア、スパム、偽情報、非表示のラベルがない
            skip_unsafe = False
            if hasattr(post, 'labels') and post.labels:
                unsafe_labels = {'porn', 'graphic-media', 'gore', 'spam', 'misleading', '!no-unauthenticated'}
                post_labels = {l.val for l in post.labels} if post.labels else set()
                if not post_labels.isdisjoint(unsafe_labels):
                    skip_unsafe = True
            if skip_unsafe:
                continue

            # [条件] アニメーションGIFや動画の除外、及び画像情報の取得
            has_gif_or_video = False
            images_info = []

            # 投稿の元データ (record) から画像のblob情報を取得
            if hasattr(embed, 'images'):
                for img_idx, img_item in enumerate(embed.images):
                    blob = img_item.image
                    mime_type = blob.mime_type
                    
                    # MIMEタイプに 'gif' や 'video' が含まれていれば除外フラグを立てる
                    if 'gif' in mime_type.lower() or 'video' in mime_type.lower():
                        has_gif_or_video = True
                        break

                    # 画像情報を構成
                    img_data = {
                        "mime_type": mime_type,
                        "size_bytes": blob.size,
                    }
                    
                    # hydrated された画像URL (post.embed) が利用可能ならフルサイズとサムネイルのURIを追加
                    if hasattr(post, 'embed') and hasattr(post.embed, 'images'):
                        if img_idx < len(post.embed.images):
                            hydrated_img = post.embed.images[img_idx]
                            img_data["fullsize_uri"] = hydrated_img.fullsize
                            img_data["thumb_uri"] = hydrated_img.thumb
                            
                    images_info.append(img_data)

            if has_gif_or_video:
                continue

            # 画像が有効に抽出できなかった場合はスキップ
            if not images_info:
                continue

            # 条件をすべてクリアした場合、情報を抽出
            post_data = {
                "cid": post.cid,
                "uri": post.uri,
                "text": getattr(record, 'text', ''),
                "created_at": getattr(record, 'created_at', ''),
                "author": {
                    "did": post.author.did,
                    "handle": post.author.handle,
                    "display_name": getattr(post.author, 'display_name', ''),
                    "avatar": getattr(post.author, 'avatar', ''),
                },
                "images": images_info
            }
            results.append(post_data)
            logging.info(f"POST: @{post_data['author']['handle']}:{post_data['text']} {post_data['created_at']}")

        except Exception as e:
            errmsg = f"投稿の解析中にエラーが発生しました: {e}"
            logging.error(errmsg)
            print(errmsg, file=sys.stderr)
            sys.exit(1)

    # DBに保存＋画像のグレイスケール化
    try:
        print("投稿画像の処理中…", flush=True)
        cur = con.cursor()
        for record in results:
            sql = "SELECT cid FROM post WHERE cid = ? AND processed = 1"
            res = cur.execute(sql, (record["cid"], ))
            if res.fetchone() is None:
                # processed = 0 のレコードが残っている可能性があるので重複対策で IGNORE
                sql = "INSERT OR IGNORE INTO post(cid, uri, message, created_at, author) VALUES(?,?,?,?,?)"
                cur.execute(sql, (record["cid"], record["uri"], record["text"], record["created_at"], record["author"]["did"], ))
                sql = "INSERT INTO author(did, handle, display_name, avatar) VALUES(?,?,?,?) ON CONFLICT(did) DO UPDATE SET handle=?, display_name=?, avatar=?"
                cur.execute(sql, (record["author"]["did"], record["author"]["handle"], record["author"]["display_name"], record["author"]["avatar"], record["author"]["handle"], record["author"]["display_name"], record["author"]["avatar"],))
            no = 1
            process_count = 0
            for img_data in record["images"]:
                sql = "SELECT id, processed FROM images WHERE cid = ? AND no = ?"
                res = cur.execute(sql, (record["cid"], no, ))
                image_rec = res.fetchone()
                if image_rec is None or image_rec['processed'] == 0:
                    # ダウンロードしてグレイスケール化する
                    monochrome = process_images(record["cid"], no, img_data["mime_type"], img_data["thumb_uri"])
                    if monochrome is not None:
                        processed = 1
                        process_count += 1
                        filename = monochrome["filename"]
                        w = monochrome["width"]
                        h = monochrome["height"]
                    else:
                        processed = 0
                        filename = None
                        w = None
                        h = None
                        
                    if image_rec is None:
                        sql = "INSERT INTO images(cid, no, mime_type, uri, thumbnail, monochrome, width, height, processed) VALUES(?,?,?,?,?,?,?,?,?)"
                        cur.execute(sql, (record["cid"], no, img_data["mime_type"], img_data["fullsize_uri"], img_data["thumb_uri"], filename, w, h, processed))
                    else:
                        sql = "UPDATE images SET monochrome = ?, width = ?, height = ?, processed = ? WHERE id = ?"
                        cur.execute(sql, (filename, w, h, processed, image_rec["id"]))
                    no += 1
            # 投稿内の画像を全件ダウンロードできたらpostを処理済みにする
            if process_count >= (no - 1):
                cur.execute("UPDATE post SET processed = 1 WHERE cid = ?", (record["cid"],))
            
    except Exception as e:
        errmsg = f'投稿の保存中にエラーが発生しました: {e}'
        logging.error(errmsg)
        print(errmsg, file=sys.stderr)
        sys.exit(1)

    con.commit()
    print("処理が終了しました", flush=True)
    logging.info(f'処理終了')

def process_images(cid, no, mime_type, thumbnail_url):
    """ 画像を16階調のグレースケールに変換して保存する。画像はKindle4の画面サイズに合わせた形でトリミングされる。 """
    # ダウンロード先のパス作成
    ext = mimetypes.guess_extension(mime_type)
    original_filename = f"{cid}_{no}{ext}"
    original_full_path = os.path.join(original_path_root, original_filename)
    # グレースケール保存先ファイル名
    processed_filename = f"{cid}_{no}.png"
    # 切り出す画像のサイズ
    image_width = int(os.getenv('IMAGE_WIDTH', 600))
    image_height = int(os.getenv('IMAGE_HEIGHT', 730))
    # 画像サイズ
    width, height = (None, None)

    # 画像のダウンロード
    try:
        response = requests.get(thumbnail_url, timeout=10)
        response.raise_for_status()
        with open(original_full_path, 'wb') as f:
            f.write(response.content)
        logging.info(f"DOWNLOAD: {thumbnail_url} -> {original_full_path}")
    except Exception as e:
        errmsg = f"ファイルのダウンロードに失敗しました: {thumbnail_url}: {e}"
        logging.error(errmsg)
        print(errmsg, file=sys.stderr)
        return None

    # 画像処理
    try:
        with Image.open(original_full_path) as img:
            # モノクロ 16 階調に変換
            # 1. 8bit グレースケール ('L' モード) に変換
            img = img.convert('L')
            # 2. 16 階調に減色 (0-255 を 16 ステップにする)
            img = img.point(lambda x: (x // 16) * 17)

            width, height = img.size
                        
            # 縦長か横長かの判定 (縦 > 横 を縦長とする)
            if height > width:
                # 縦長の場合
                # 横が 600px 以上であれば 600px に収まるように縮小
                if width >= image_width:
                    new_width = image_width
                    new_height = int(height * (image_width / width))
                    img = img.resize((new_width, new_height), Image.Resampling.LANCZOS)
                    width, height = img.size
                            
                # 縮小の結果、縦が 730px を超えた場合は 730px にトリミング
                if height > image_height:
                    # 中央を基準に 730px を残すトリミング
                    oy = round((height - image_height) / 2)
                    img = img.crop((0, oy, width, image_height + oy))
            else:
                # 横長の場合 (または正方形)
                # 縦が 730px 以上であれば 730px に収まるように縮小
                if height >= image_height:
                    new_height = image_height
                    new_width = int(width * (image_height / height))
                    img = img.resize((new_width, new_height), Image.Resampling.LANCZOS)
                    width, height = img.size
                            
                # 縮小の結果、幅が 600px を超えた場合は 600px にトリミング
                if width > image_width:
                    # 中央を基準に 600px を残すトリミング
                    ox = round((width - image_width) / 2)
                    img = img.crop((ox, 0, image_width + ox, height))

            # 保存
            processed_full_path = os.path.join(processed_path_root, processed_filename)
            img.save(processed_full_path, "PNG")
            logging.info(f"PROCESSED: {original_full_path} -> {processed_full_path}")
            
            # 画像サイズを再取得
            width, height = img.size

    except Exception as e:
        errmsg = f"ファイルの変換および保存に失敗しました: {original_full_path}: {e}"
        logging.error(errmsg)
        print(errmsg, file=sys.stderr)
        return None

    return { "filename": processed_filename, "width": width, "height": height }

def create_schema():
    """ 使用するテーブルを作成する """
    try:
        cur = con.cursor()
        sql = "SELECT name FROM sqlite_master WHERE name='post'"
        res = cur.execute(sql)
        if res.fetchone() is None:
            logging.info('postテーブルを作成')
            sql = "CREATE TABLE post(id INTEGER PRIMARY KEY AUTOINCREMENT, cid TEXT UNIQUE, uri TEXT, message TEXT, created_at TEXT, author TEXT, processed INTEGER DEFAULT 0) "
            cur.execute(sql)
            sql = "CREATE INDEX post_created_at ON post(created_at) "
            cur.execute(sql)
            logging.info('authorテーブルを作成')
            sql = "CREATE TABLE author(did TEXT PRIMARY KEY, handle TEXT, display_name TEXT, avatar TEXT) "
            cur.execute(sql)
            logging.info('imagesテーブルを作成')
            sql = "CREATE TABLE images(id INTEGER PRIMARY KEY AUTOINCREMENT, cid TEXT, no INTEGER, mime_type TEXT, uri TEXT, thumbnail TEXT, monochrome TEXT, width INTEGER, height INTEGER, processed INTEGER DEFAULT 0) "
            cur.execute(sql)
    except Exception as e:
        errmsg = f'テーブル作成中にエラーが発生しました: {e}'
        logging.error(errmsg)
        print(errmsg, file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    load_dotenv()
    logfile = os.getenv('LOGFILE')
    db_name = os.getenv('DB')

    # ログの作成
    logging.basicConfig(filename=logfile, format='%(asctime)s %(levelname)s:%(message)s', encoding='utf-8', level=logging.DEBUG)

    # DB読み込み
    con = sqlite3.connect(db_name)
    con.row_factory = sqlite3.Row
    create_schema()

    # 画像保存先ディレクトリ
    original_path_root = os.getenv('ORIGINAL_IMAGE_PATH')
    processed_path_root = os.getenv('PROCESSED_IMAGE_PATH')
    # ディレクトリの作成
    os.makedirs(original_path_root, exist_ok=True)
    os.makedirs(processed_path_root, exist_ok=True)

    main()

    con.close()
