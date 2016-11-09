package main

import "html/template"

var indexTmpl = template.Must(template.New("index").Parse(`
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
		img            { float: left; max-width: 100px }
		p              { margin: 0 0 0.22em 0; }
		p.details      { font-size: smaller; }
		section        { margin-bottom: 1em; }
		.record        { clear: both; border-bottom: 1px solid #888; margin:0; padding: 0.5em;}
		.record-img    { display: inline-block; width: 20%; float; left; padding-top: 0.5em; }
		.record-text   { display: inline-block; width: 78%; float; right; vertical-align: top }
		.record:hover  { background-color: #f0f0f0; }
		.xmlRecord     { display: block; float: right; font-variant: small-caps }
		.collections span+span:before,.subjects span+span:before { content: ' | '}
		.subtitles small+small:before { content: ' : '}
		.pagination    { text-align: center; }
		.pagination ul { display: inline-block; list-style-type: none; margin: 0; padding: 0; }
		.pagination li { display: inline-block; float: left; margin: 1em }
		@media print { body { max-width:none } }
	</style>
</head>
<body>
	<article>
		<header>
			<h1>Otra</h1>
		</header>
		<section class="relative">
			<form id="searchForm" action="/">
				<input list="suggestions" id="search" type="text" autocomplete="off" name="q" value="{{.Query}}" /> <button id="searchButton" type="submit">Søk</button>
			</form>
			<datalist id="suggestions"></datalist>
		</section>
		{{if .Query}}
			<section id="hits">
				<h4>{{.Total}} hits ({{.Took}}ms)</h4>
				{{range .Hits}}
					<div class="record">
						<div class="record-img">
							{{if .HasImage}}
								<a target="_blank" href="/img/{{.ID}}"><img src="/img/{{.ID}}/os.jpg"></a>
							{{end}}
						</div>
						<div class="record-text">
							<div class="xmlRecord"><a target="_blank" href="/record/{{.ID}}">xml</a> </div>
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
					</div>
				{{end}}
				<div class="pagination">
					<ul>
						{{$results := .}}
						{{range .Pages}}
							<li>
								{{if .Active}}
									<strong>{{.Page}}</strong>
								{{else}}
									<a href="/?q={{$results.Query}}&page={{.Page}}">{{.Page}}</a>
								{{end}}
							</li>
						{{end}}
					</ul>
				</div>
			</section>
		{{end}}
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


		document.getElementById('search').addEventListener('keyup', keyPressed)
		getIndexes()

	</script>
</body>
</html>
`))

var statsTmpl = template.Must(template.New("stats").Parse(`<pre>
Database
========
path: {{.Path}}
size: {{.Size}}
records: {{.Records}}

Indexes
=======
{{range .Indexes -}}
{{.Name}}: {{.Count}}
{{end}}
</pre>
`))
