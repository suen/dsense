//mapreduce for histogram of tweets

var mapper = function() {
	var histo = this.histo;

	if (histo) {
		for(w in histo) {
			emit(w.trim(), {count: histo[w], id: [this._id]} );
		}
	}
}

var reducer = function(word, reducedSet) {
	
	reduced = { count: 0, id: [] };

	for(idx = 0; idx < reducedSet.length; idx++) {
		reduced.count += reducedSet[idx].count++;

		for(ids = 0; ids < reducedSet[idx].id.length; ids++)
			reduced.id.push(reducedSet[idx].id[ids]);
	}

	return reduced;
}

