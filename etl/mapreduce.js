//mapreduce for histogram of tweets

var wordCounter = function(word, countArray) {
	return Array.sum(countArray);
}

var we2 = function() {
	
	var text = this.text;
	if (text) {
		text = text.toLowerCase().split(" ");

		for (i = 0; i<text.length; i++) {
			text[i] = text[i].trim();	
			emit(text[i], {count: 1, id: [this._id]});
		}

	}
}

var we3 = function() {
	var histo = this.histo;

	if (histo) {
		for(w in histo) {
			emit(w.trim(), {count: histo[w], id: [this._id]} );
		}
	}
}

var wc2 = function(word, reducedSet) {
	
	reduced = { count: 0, id: [] };

	for(idx = 0; idx < reducedSet.length; idx++) {
		reduced.count += reducedSet[idx].count++;

		for(ids = 0; ids < reducedSet[idx].id.length; ids++)
			reduced.id.push(reducedSet[idx].id[ids]);
	}

	return reduced;
}

var histogram = function(sentence) {

	if (sentence) {
		words = sentence.toLowerCase().split(" ");
		histo = {};
		for(i=0; i<words.length; i++) {
			if (words[i].substr(0,4) == "http")
				continue;
			p = /[a-zA-Z0-9 #@]/
			if (! p.test(words[i]))
				continue;
			if (histo.hasOwnProperty(words[i]))
				histo[words[i]]++;
			else
				histo[words[i]] = 1;
		}
		return histo;
	}
}
