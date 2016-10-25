package main

import "html/template"

var page = []byte(`
<!DOCTYPE html>
<html>
<head>
	<meta charset="utf-8">
	<meta name="viewport" content="width=device-width, initial-scale=1">
	<title>Otra</title>
	<style>
		*              { box-sizing: border-box }
		body           { margin:1em auto; max-width:40em; padding: 0 .62em; font:1.2em/1.62em sans-serif; }
		a, a:visited   { color: blue; }
		h1, h2, h3, h4 { line-height:1.2em; }
		label          { display: block; font-weight: bold; font-size: 80%; }
		input          { width: 60% }
		input, button  { padding: .2em .62em; font-size: 100% }
		.relative      { position: relative }
		.hidden        { display: none }
		p              { margin: 0 0 0.22em 0; }
		p.details      { font-size: smaller; }
		section		   { margin-bottom: 1em; }
		.record        { border-bottom: 1px dashed #000; margin:0; padding: 0.5em ;}
		.record:hover  { background-color: #f0f0f0; }
		.xmlRecord     { display: block; float: right; font-variant: small-caps }
		.collections span+span:before,.subjects span+span:before { content: ' | '}
		.subtitles small+small:before { content: ' : '}

		@media print { body { max-width:none } }
	</style>
</head>
<body>
	<article>
		<header>
			<h1>Otra</h1>
		</header>
		<section class="relative">
			<form id="searchForm">
				<input list="suggestions" id="search" type="text" autocomplete="off" /> <button id="searchButton" type="submit">Søk</button>
			</form>
			<datalist id="suggestions"></datalist>
		</section>
		<section id="hits">
		</section>
	</article>
	<script>
		// global state
		var indexes = []

		function setDatalist(options) {
			var sugNode = document.getElementById("suggestions")
			while (sugNode.firstChild) {
			    sugNode.removeChild(sugNode.firstChild)
			}
			options.forEach(function(el) {
				var option = document.createElement('option');
				option.innerText = el
				sugNode.appendChild(option);
			})
		}

		function getIndexes() {
			var req = new XMLHttpRequest()
			req.open('GET', '/indexes')
			req.onload = function(resp) {
				if (req.status >= 200 && req.status < 400) {
					indexes = JSON.parse(req.responseText).map(function(el) {
						return el+'/'
					})
					setDatalist(indexes)
				} else {
					console.log(req.status)
				}
				return true
			}

			req.onerror = function() {
				console.log("connection error")
			}
			req.send()
		}

		function doSearch(event) {
			var q = document.getElementById('search').value
			var parts = document.getElementById('search').value.split('/')
			var index = parts[0]
			var query = parts[1]

			var req = new XMLHttpRequest()
			req.open('GET', '/query/'+index+'/'+query, true)
			req.onload = function(resp) {
				if (req.status >= 200 && req.status < 400) {
					document.getElementById('hits').innerHTML = req.responseText
					if ( '?q='+q !== document.location.search) {
						// TODO understand why this check is needed to avoid double history entries
						history.pushState(q, null, '/?q='+q)
					}
				} else {
					console.log(req.status)
				}
				return true
			}

			req.onerror = function() {
				console.log("connection error")
			}
			req.send()

			return event.preventDefault()
		}

		function keyPressed(event) {
			if (event.keyCode <= 40 && event.keyCode >= 37) {
				return true
			}
			var q = document.getElementById('search').value
			var i = q.indexOf('/')
			if ( i == -1) {
				setDatalist(indexes)
				return true
			}
			if (i+1 === q.length) {
				return true
			}
			var req = new XMLHttpRequest()
			req.open('GET', '/autocomplete/'+q, true)
			req.onload = function(resp) {
				if (req.status >= 200 && req.status < 400) {
					suggestions = JSON.parse(req.responseText) || []
					suggestions = suggestions.map(function(el) {
						return q.substring(0, i+1)+el
					})
					setDatalist(suggestions)
				} else {
					console.log(req.status)
				}
				return true
			}

			req.onerror = function() {
				console.log("connection error")
			}
			req.send()



		}

		document.getElementById('searchForm').addEventListener('submit', doSearch)
		document.getElementById('hits').addEventListener('click', function(event) {
			var href = event.target.getAttribute('href')
			if (href && href.startsWith('/?q=')) {
				event.preventDefault()
				document.getElementById('search').value = decodeURIComponent(href.substring(4))
				document.getElementById('searchButton').click()
			}
		})


		if ( window.location.search.startsWith('?q=') ) {
			document.getElementById('search').value = decodeURIComponent(window.location.search.substring(3))
			document.getElementById('searchButton').click()
		}

		history.replaceState(document.getElementById('search').value, null, document.location.href);

		window.addEventListener('popstate', function(event) {
			if (!event.state || event.state === document.getElementById('search').value) {
				return
			}
			document.getElementById('search').value = event.state
			document.getElementById('searchButton').click()
		})

		document.getElementById('search').addEventListener('keyup', keyPressed)
		getIndexes()

	</script>
</body>
</html>
`)

var hitsTmpl = template.Must(template.New("hits").Parse(`
{{range .}}
	<div class="record">
		<a class="xmlRecord" target="_blank" href="/record/{{.ID}}">xml</a>
		<p><strong>{{.Title}}</strong><br/>
			<span class="subtitles">{{range .Subtitles}}<small>{{.}}</small>{{end}}</span>
		</p>
		<p class="contributors">
			{{range $role, $agents := .Contributors}}
				<span>{{$role}} {{range $agents}}<a href="/?q=agent/{{.}}">{{.}}</a> {{end}}</span>
			{{end}}
		</p>
		<p class="details">Utgitt av {{.Publisher}} <a href="/?q=year/{{.PublishedYear}}">{{.PublishedYear}}</a></p>
		{{if .Collection}}
			<p class="collections details">Serie:
				{{range .Collection}}<span><a href="/?q=series/{{.}}">{{.}}</a></span>{{end}}
			</p>
		{{end}}
		{{if .Subjects}}
			<p class="subjects details">Emner:
				{{range .Subjects}}<span><a href="/?q=subject/{{.}}">{{.}}</a></span>{{end}}
			</p>
		{{end}}
	</div>
{{end}}
`))
