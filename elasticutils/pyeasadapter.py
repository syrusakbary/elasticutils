import pyes

class PyesElasticSearchAdapter(object):
    def __init__(self, urls, timeout, **settings):
        self.urls = urls
        self.timeout = timeout
        self.settings = settings
        self.conn = pyes.ES(urls)

    def refresh(self, index):
        self.conn.optimize(indices=index, wait_for_merge=True, refresh=True)

    def more_like_this(self, index, doc_type, id, mlt_fields, body=None, **params):
        return self.conn.morelikethis(index, doc_type, id, fields=mlt_fields, body=body, mlt_fields=','.join(mlt_fields), **params)

    def delete(self, index, doc_type, id):
        self.conn.delete(index, doc_type, id)

    def index(self, index, doc_type, document, id, overwrite_existing=True):
        self.conn.index(document, index, doc_type, id=id)

    def bulk_index(self, index, doc_type, documents, id_field):
        for document in documents:
            id = document[id_field]
            self.conn.index(document, index, doc_type, id=id, bulk=True)
        self.conn.flush_bulk(forced=True)

    def search(self, query, index, doc_type):
        return self.conn.search_raw(query, indices=index, doc_types=doc_type)

    def health(self):
        return self.conn.cluster_health()

    def create_index(self, index, settings=None):
        self.conn.create_index_if_missing(index, settings=settings)

    def delete_index(self, index):
        self.conn.delete_index(index)

