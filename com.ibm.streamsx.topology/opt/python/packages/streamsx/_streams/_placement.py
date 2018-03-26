# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017,2018
from future.builtins import *

class _Placement(object):

    @property
    def category(self):
        """Category for this processing logic.

        An arbitrary application label allowing grouping of application
        elements by category.

        Assign categories based on common function.
        For example, `database` is a common category that you can
        use to group all database sinks in an application.

        A category is not required and defaults to ``None`` meaning
        no assigned category.

        Streams console supports visualization based upon categories.

        Raises:
            TypeError: No directly associated processing logic.

        .. note:: A category has no affect on the execution of the application.

        .. versionadded:: 1.9
        """
        try:
            return self._op().category
        except TypeError:
            return None

    @category.setter
    def category(self, value):
        self._op().category = value

    @property
    def resource_tags(self):
        """Resource tags for this processing logic.

        Tags are a mechanism for differentiating and identifying resources that have different physical characteristics or logical uses. For example a resource (host) that has external connectivity for public data sources may be tagged `ingest`.

        Processing logic can be associated with one or more tags to require 
        running on suitably tagged resources. For example
        adding tags `ingest` and `db` requires that the processing element
        containing the callable that created the stream runs on a host
        tagged with both `ingest` and `db`.

        A :py:class:`~streamsx.topology.topology.Stream` that was not created directly with a Python callable
        cannot have tags associated with it. For example a stream that
        is a :py:meth:`~streamsx.topology.topology.Stream.union` of multiple streams cannot be tagged.
        In this case this method returns an empty `frozenset` which
        cannot be modified.

        See https://www.ibm.com/support/knowledgecenter/en/SSCRJU_4.2.1/com.ibm.streams.admin.doc/doc/tags.html for more details of tags within IBM Streams.

        Returns:
            set: Set of resource tags, initially empty.

        .. warning:: If no resources exist with the required tags then job submission will fail.
        
        .. versionadded:: 1.7
        .. versionadded:: 1.9 Support for :py:class:`Sink` and :py:class:`~streamsx.spl.op.Invoke`.
   
        """
        try:
            plc = self._op()._placement
            if not 'resourceTags' in plc:
                plc['resourceTags'] = set()
            return plc['resourceTags']
        except TypeError:
            return frozenset()

    def colocate(self, others):
        """Colocate this processing logic with others.

        Colocating processing logic requires execution in
        the same Streams processing element (operating system process).

        When a job is submitted Streams may colocate (fuse) processing
        logic into the same processing element based upon flow analysis
        and current resource usage. This call instructs that this logic
        and `others` must be executed in the same processing element.

        Args:
            others: Processing logic such as a
                :py:class:`~streamsx.topology.topology.Stream`
                or :py:class:`~streamsx.topology.topology.Sink`.
                A single value can be passed or an iterable, such
                as a list of streams.

        Returns:
            self: This logic.

        .. versionadded: 1.9
        """
        if not others:
            return self
        return self._colocate(others, 'colocate')

    def _colocate(self, others, why):
       
        other_ops = list()
        try:
            for p in others:
                try:
                    other_ops.append(p._op())
                except TypeError: # Only add placeables
                    pass
        except TypeError: # not an iterable
            try:
                other_ops.append(others._op())
            except TypeError:
                pass
 
        self._op().colocate(other_ops, 'colocate')
        return self
