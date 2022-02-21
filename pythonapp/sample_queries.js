[{
	$lookup: {
		from: 'charactersToComics',
		localField: 'comicID',
		foreignField: 'comicID',
		as: 'characters'
	}
}, {
	$lookup: {
		from: 'characters',
		localField: 'characters.characterID',
		foreignField: 'characterID',
		as: 'characters'
	}
}, {
	$out: 'comics_and_characters'
}]


[{
    $lookup: {
        from: 'charcters_stats',
        localField: 'Name',
        foreignField: 'Name',
        as: 'Stats'
    }
}, {
    $lookup: {
        from: 'superheroes_power_matrix',
        localField: 'Name',
        foreignField: 'Name',
        as: 'Powers'
    }
}, {
    $out: 'characters_sheet'
}]
