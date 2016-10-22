package main

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
		input          { padding: .2em .62em; font-size: 100% }
		.row           { clear: both }
		.col           { width: 50%; float: left }

		@media print { body { max-width:none } }
	</style>
</head>
<body>
	<article>
		<header>
			<h1>Otra</h1>
		</header>
		<section class="row">
			<div class="col">
				<datalist id="indexes">
					<option>agent</option>
					<option>author</option>
					<option>collection</option>
					<option>editor</option>
					<option>illustrator</option>
					<option>isbn</option>
					<option>subject</option>
					<option>title</option>
					<option>translator</option>
					<option>year</option>
				</datalist>
				<datalist id="hits"></datalist>
				<label>Index</label>
				<input id="index" type="text" list="indexes" />
			</div>
			<div class="col">
				<label>Query</label>
				<input id="query" type="text" list="hits" />
			</div>
		</section>
		<section>
		</section>
	</article>
	<script>
		function keyPressed(event) {
			var index = document.getElementById('index').value
			var query = document.getElementById('query').value
			if (index == '' || query === '') {
				return
			}
			var req = new XMLHttpRequest()
			req.open('GET', '/autocomplete/'+index+'/'+query, true)
			req.onload = function() {
				if (req.status >= 200 && req.status < 400) {
					var hits = req.responseText.split('\n')
					var hitsList = document.getElementById('hits')
					while (hitsList.firstChild) {
					    hitsList.removeChild(hitsList.firstChild)
					}
					hits.forEach(function(hit) {
						var option = document.createElement('option')
				        option.value = hit;
				        hitsList.appendChild(option);
					})
				} else {
					console.log(req.status)
			  	}
			}

			req.onerror = function() {
				console.log("connection error")
			}
			req.send()
		}
		var input = document.getElementById('query')
		input.addEventListener('keyup', keyPressed)
	</script>
</body>
</html>

`)
