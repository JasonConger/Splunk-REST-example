import os
import sys
import time
import datetime
import httplib2
import urllib
import json

from splunklib.modularinput import *
from state_store import FileStateStore

class MyScript(Script):

	def get_scheme(self):

		scheme = Scheme("Splunk REST Example")
		scheme.description = ("Collects and checkpoints REST data.")
		scheme.use_external_validation = True
		scheme.streaming_mode_xml = True
		scheme.use_single_instance = False

		# This is one way to create an argument for your input
		url_arg = Argument("rest_url")
		url_arg.title = "REST URL"
		url_arg.data_type = Argument.data_type_string
		url_arg.description = "The REST endpoint without parameters"
		url_arg.required_on_create = True
		url_arg.required_on_edit = True
		scheme.add_argument(url_arg)

		# This is another way to create an arguement for your input
		num_arg = Argument(
			name="num",
			title="Number of posts",
			data_type=Argument.data_type_number,
			description="Number of posts to return on each poll.",
			required_on_create=True,
			required_on_edit=True
		)
		scheme.add_argument(num_arg)

		return scheme

	def validate_input(self, definition):
		# Try to retrieve some data from the endpoint
		pass

	def stream_events(self, inputs, ew):
		self.input_name, self.input_items = inputs.inputs.popitem()
		rest_url = self.input_items['rest_url']
		num      = self.input_items['num']

		try:
			state_store = FileStateStore(inputs.metadata, self.input_name)
			# Get the last position read (if any) - defaults to 0
			last_position = state_store.get_state("last_position") or 0
			
			self.header = {}
			self.data = {}
			self.url = "%s?start=%s&num=%s" % (rest_url, str(last_position), str(num))
			self.rest_method = 'GET'

			http_cli = httplib2.Http(timeout=10, disable_ssl_certificate_validation=True)
			resp, content = http_cli.request(self.url, method=self.rest_method, body=urllib.urlencode(self.data), headers=self.header)
			
			jsVariable = content.decode('utf-8', errors='ignore')
			
			# The response from this particular REST endpoint delivers content in a JavaScript variable.
			#   - Example: tumblr_api_read = {"key":value, "key":value, "posts":[array of posts]}
			#
			# The below line strips out the unnecessary text to get just the JSON
			jsonValue = json.loads('{%s}' % (jsVariable.split('{', 1)[1].rsplit('}', 1)[0],))
			
			num_posts_streamed = 0
			for post in jsonValue["posts"]:
				num_posts_streamed += 1
				event = Event()
				event.stanza = self.input_name
				event.data = json.dumps(post)
				ew.write_event(event)
				
			# Store the position to pick up on next time
			last_position = int(last_position) + num_posts_streamed
			state_store.update_state("last_position", str(last_position))

		except Exception as e:
			raise e

if __name__ == "__main__":
	exitcode = MyScript().run(sys.argv)
	sys.exit(exitcode)
