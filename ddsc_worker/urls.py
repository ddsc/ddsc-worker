# (c) Nelen & Schuurmans. MIT licensed, see LICENSE.rst.

from django.conf.urls.defaults import include
from django.conf.urls.defaults import patterns
from django.conf.urls.defaults import url
from django.contrib import admin
from django.http import HttpResponseRedirect

admin.autodiscover()

urlpatterns = patterns('',
    (r'^$', lambda x: HttpResponseRedirect('/admin/')),
    url(r'^admin/', include(admin.site.urls)),
)
