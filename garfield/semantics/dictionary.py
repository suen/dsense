from gensim import corpora
from six import PY3, iteritems, iterkeys, itervalues, string_types
from patterns import Singleton

class SuperDictionary:
	def __init__(self):
		words = [ x.strip() for x in open("dictnostops.txt").readlines() ]
		self.dictionary = corpora.Dictionary()
		self.dictionary.add_documents([words])

@Singleton
class WrapperDictionary:
	def init(self, size):
		self.super = corpora.Dictionary()
		self.token2id = self.super.token2id
		self.id2token = self.super.id2token
		self.dfs = self.super.dfs
		self.num_docs = self.super.num_docs
		self.num_pos = self.super.num_pos
		self.num_nnz = self.super.num_nnz

		self.index = 0
		self.prefix = "uzumymw"
		self.size = size
		self.numlist = self.createNumberList(size)
		self.super.add_documents([self.numlist])
		print "Wrapper Dictionary initialized"
	
	def createNumberList(self, upperbound):
		strnum = []
		for i in range(upperbound):
			strnum.append(self.prefix+str(i))
		return strnum

	def add_word(self, word):
		if self.index >= self.size:
			return False

		token2id = self.super.token2id

		id = self.super.token2id[self.prefix+str(self.index)]
		
		self.super.token2id[word] = id

		del self.super.token2id[self.prefix+str(self.index)]
		self.index += 1

		self.super.id2token = dict((v, k) for k, v in iteritems(self.super.token2id))
		return True
		

	def __getitem__(self, tokenid):
		return self.super.__getitem__(tokenid)


	def __iter__(self):
		return self.super.__iter__()

	def keys(self):
		return self.super.keys()

	def __len__(self):
		return self.super.__len__()


	def __str__(self):
		return self.super.__str__()

	@staticmethod
	def from_documents(documents):
		return self.super.from_documents(documents)

	def add_documents(self, documents, prune_at=2000000):
		self.super.add_documents(documents, prune_at)

	def doc2bow(self, document, allow_update=False, return_missing=False):
		return self.super.doc2bow(document, allow_update, return_missing)

	def filter_extremes(self, no_below=5, no_above=0.5, keep_n=100000):
		self.super.filter_extremes(no_below, no_above, keep_n)

	def filter_tokens(self, bad_ids=None, good_ids=None):
		self.super.filter_tokens(bad_ids, good_ids)

	def compactify(self):
		self.super.compactify()

	def save_as_text(self, fname, sort_by_word=True):
		self.super.save_as_text(fname, sort_by_word)

	@staticmethod
	def merge_with(self, other):
		return self.super.merge_with(other)

	@staticmethod
	def load_from_text(fname):
		return self.super.load_from_text(fname)

	@staticmethod
	def from_corpus(corpus, id2word=None):
		return self.super.from_corpus(corpus, id2word)
