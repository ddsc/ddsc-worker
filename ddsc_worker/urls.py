# (c) Nelen & Schuurmans. MIT licensed, see LICENSE.rst.

from django.conf.urls.defaults import include
from django.conf.urls.defaults import patterns
from django.conf.urls.defaults import url
from django.contrib import admin
##from lizard_ui.urls import debugmode_urlpatterns

admin.autodiscover()

urlpatterns = patterns(
    '',
    url(r'^admin/', include(admin.site.urls)),
    # url(r'^something/',
    #     views.some_method,
    #     name="name_it"),
    # url(r'^something_else/$',
    #     views.SomeClassBasedView.as_view(),
    #     name='name_it_too'),
)

##urlpatterns += debugmode_urlpatterns()
