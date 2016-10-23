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
		input, button  { padding: .2em .62em; font-size: 100% }
		p              { margin: 0 0 0.22em 0; }
		p.details      { font-size: smaller; }
		section		   { margin-bottom: 1em }
		.record        { border-bottom: 1px dashed #000; margin:0; padding: 0.5em ;}
		.record:hover  { background-color: #f0f0f0; }
		.xmlRecord     { display: block; float: right; font-variant: small-caps }
		.collections span+span:before,.subjects span+span:before { content: ' | '}
		@media print { body { max-width:none } }
	</style>
</head>
<body>
	<article>
		<header>
			<h1>Otra</h1>
		</header>
		<section>
			<form id="searchForm">
				<input id="search" type="text" autocomplete="off" /> <button id="searchButton" type="submit">Søk</button>
			</form>
		</section>
		<section id="hits">
		</section>
	</article>
	<script>
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

	</script>
</body>
</html>
`)

var hitsTmpl = template.Must(template.New("hits").Parse(`
{{range .}}
	<div class="record">
		<a class="xmlRecord" target="_blank" href="/record/{{.ID}}">xml</a>
		<p><strong>{{.Title}}</strong><br/>
			{{if .Subtitle}}<small>{{.Subtitle}}</small>{{end}}
		</p>
		<p class="contributors">
			{{range $role, $agents := .Contributors}}
				<span>{{$role}} {{range $agents}}<a href="/?q=agent/{{.}}">{{.}}</a> {{end}}</span>
			{{end}}
		</p>
		<p class="details">Utgitt av {{.Publisher}} {{.PublishedYear}}</p>
		{{if .Collection}}
			<p class="collections details">Serie:
				{{range .Collection}}<span>{{.}}</span>{{end}}
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
