import os
from bottle import route, run, template, static_file, abort, redirect
import sqlite3
from dotenv import load_dotenv
import dateutil.parser
import zoneinfo

@route('/view/<id>')
def view(id):
    """ 指定されたidの画像を表示 """
    cur = con.cursor()
    sql = "SELECT * FROM images WHERE id = ? AND processed = 1"
    res = cur.execute(sql, (id, ))
    image = res.fetchone()
    if image is not None:
        sql = "SELECT * FROM post WHERE cid = ?"
        post_res = cur.execute(sql, (image['cid'], ))
        post = post_res.fetchone()
        sql = "SELECT * FROM author WHERE did = ?"
        author_res = cur.execute(sql, (post['author'], ))
        author = author_res.fetchone()
        alt = f'@{author['handle']} ({author['display_name']}): {post['message']}'
        url = at_uri_to_http(post['uri'])

        # JSTに変換
        dt = dateutil.parser.parse(post['created_at'])
        created_at = dt.astimezone(zoneinfo.ZoneInfo('Asia/Tokyo')).strftime('%Y-%m-%d %H:%M:%S')
        
        # 次のページを検索
        next_id = None
        # 同じ投稿の次の画像がないか探す
        sql = "SELECT * FROM images WHERE cid = ? AND no > ? AND processed = 1"
        next_res = cur.execute(sql, (image['cid'], image['no'], ))
        next_image = next_res.fetchone()
        if next_image is not None:
            next_id = next_image['id']
        else:
            # なかった場合は一つ前の投稿を探す
            sql = "SELECT cid FROM post WHERE processed = 1 AND created_at < ? ORDER BY created_at DESC LIMIT 1"
            next_res = cur.execute(sql, (post['created_at'], ))
            next_post = next_res.fetchone()
            if next_post is not None:
                sql = "SELECT id FROM images WHERE cid = ? AND no = 1 LIMIT 1"
                next_res = cur.execute(sql, (next_post['cid'], ))
                next_image = res.fetchone()
                if next_image is not None:
                    next_id = next_image['id']
        # それもなかった場合は一番最初に戻る
        if next_id is None:
            next_id = get_first_id()
        
        # 縦と横のマージンを計算
        image_height = int(os.getenv('IMAGE_HEIGHT', 740))
        height = image["height"]
        if image_height > height:
            margin_top = round((image_height - height) / 2)
        else:
            margin_top = 0
            
        image_width = int(os.getenv('IMAGE_WIDTH', 600))
        width = image["width"]
        if image_width > width:
            margin_left = round((image_width - width) / 2)
        else:
            margin_left = 0

        return template('index', filename=image['monochrome'], width=image["width"], margin_top=margin_top, margin_left=margin_left, alt=alt, author=author, post=post, url=url, next_id=next_id, created_at=created_at)
    else:
        # ページが見つからない場合、一番新しい投稿を探してリダイレクトする
        id = get_first_id()
        if id is not None:
           redirect(f"/view/{id}")
        else:
            abort(404, "ページが見つかりません。")

@route('/images/<file_path:path>')
def static(file_path):
    """ 画像ファイルが入っているディレクトリのファイルを返す """
    processed_path_root = os.getenv('PROCESSED_IMAGE_PATH')
    return static_file(file_path, root=processed_path_root)
    
@route('/')
def home():
    """ 一番新しい投稿を探してリダイレクトする """
    id = get_first_id()
    if id is not None:
       redirect(f"/view/{id}")
    else:
        return "表示する内容がありません！"

def get_first_id():
    """ 一番新しい投稿を探す """
    id = None
    cur = con.cursor()
    sql = "SELECT cid FROM post WHERE processed = 1 ORDER BY created_at DESC LIMIT 1"
    res = cur.execute(sql)
    rec = res.fetchone()
    if rec is not None:
        sql = "SELECT id FROM images WHERE cid = ? AND no = 1 LIMIT 1"
        res = cur.execute(sql, (rec['cid'], ))
        rec = res.fetchone()
        if rec is not None:
            id = rec['id']
    return id

def at_uri_to_http(at_uri: str) -> str:
    """ ATプロトコルのURIをBlueskyのHTTPSのURLに変換する """
    if not at_uri.startswith("at://"):
        return at_uri

    # at:// を取り除き、各要素に分割
    parts = at_uri.replace("at://", "").split("/")
    
    # 基本構造: [repo(DID), collection, rkey]
    if len(parts) < 3:
        return at_uri
    
    repo, collection, rkey = parts[0], parts[1], parts[2]
    base_url = f"https://bsky.app/profile/{repo}"

    # コレクション名（名前空間）に応じてパスを切り替える
    if collection == "app.bsky.feed.post":
        return f"{base_url}/post/{rkey}"
    elif collection == "app.bsky.feed.generator":
        return f"{base_url}/feed/{rkey}"
    elif collection == "app.bsky.graph.list":
        return f"{base_url}/lists/{rkey}"
    else:
        # 未知のコレクションの場合はそのまま返す
        return at_uri

if __name__ == "__main__":
    # .env ファイルの読み込み
    load_dotenv()
    db_path = os.getenv('DB')

    # SQLite3 接続
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row

    # Webサーバ実行
    run(host='0.0.0.0', port=8080)
