if (typeof jQuery !== 'undefined') {
    /*!
     * Bootstrap v3.3.1 (http://getbootstrap.com)
     * Copyright 2011-2014 Twitter, Inc.
     * Licensed under MIT (https://github.com/twbs/bootstrap/blob/master/LICENSE)
     */

    /*!
     * Generated using the Bootstrap Customizer (http://getbootstrap.com/customize/?id=698666b23215c58f23d4)
     * Config saved to config.json and https://gist.github.com/698666b23215c58f23d4
     */

    /* ========================================================================
     * Bootstrap: tooltip.js v3.3.1
     * http://getbootstrap.com/javascript/#tooltip
     * Inspired by the original jQuery.tipsy by Jason Frame
     * ========================================================================
     * Copyright 2011-2014 Twitter, Inc.
     * Licensed under MIT (https://github.com/twbs/bootstrap/blob/master/LICENSE)
     * ======================================================================== */


    +function ($) {
      'use strict';

      if(typeof $().tooltip == 'function')
        return true;

      // TOOLTIP PUBLIC CLASS DEFINITION
      // ===============================

      var Tooltip = function (element, options) {
        this.type       =
        this.options    =
        this.enabled    =
        this.timeout    =
        this.hoverState =
        this.$element   = null;

        this.init('tooltip', element, options);
      };

      Tooltip.VERSION  = '3.3.1';

      Tooltip.TRANSITION_DURATION = 150;

      Tooltip.DEFAULTS = {
        animation: true,
        placement: 'top',
        selector: false,
        template: '<div class="tooltip" role="tooltip"><div class="tooltip-arrow"></div><div class="tooltip-inner"></div></div>',
        trigger: 'hover focus',
        title: '',
        delay: 0,
        html: false,
        container: false,
        viewport: {
          selector: 'body',
          padding: 0
        }
      };

      Tooltip.prototype.init = function (type, element, options) {
        this.enabled   = true;
        this.type      = type;
        this.$element  = $(element);
        this.options   = this.getOptions(options);
        this.$viewport = this.options.viewport && $(this.options.viewport.selector || this.options.viewport);

        var triggers = this.options.trigger.split(' ');

        for (var i = triggers.length; i--;) {
          var trigger = triggers[i];

          if (trigger == 'click') {
            this.$element.on('click.' + this.type, this.options.selector, $.proxy(this.toggle, this));
          } else if (trigger != 'manual') {
            var eventIn  = trigger == 'hover' ? 'mouseenter' : 'focusin';
            var eventOut = trigger == 'hover' ? 'mouseleave' : 'focusout';

            this.$element.on(eventIn  + '.' + this.type, this.options.selector, $.proxy(this.enter, this));
            this.$element.on(eventOut + '.' + this.type, this.options.selector, $.proxy(this.leave, this));
          }
        }

        this.options.selector ?
          (this._options = $.extend({}, this.options, { trigger: 'manual', selector: '' })) :
          this.fixTitle();
      };

      Tooltip.prototype.getDefaults = function () {
        return Tooltip.DEFAULTS;
      };

      Tooltip.prototype.getOptions = function (options) {
        options = $.extend({}, this.getDefaults(), this.$element.data(), options);

        if (options.delay && typeof options.delay == 'number') {
          options.delay = {
            show: options.delay,
            hide: options.delay
          };
        }

        return options;
      };

      Tooltip.prototype.getDelegateOptions = function () {
        var options  = {};
        var defaults = this.getDefaults();

        this._options && $.each(this._options, function (key, value) {
          if (defaults[key] != value) options[key] = value;
        });

        return options;
      };

      Tooltip.prototype.enter = function (obj) {
        var self = obj instanceof this.constructor ?
          obj : $(obj.currentTarget).data('bs.' + this.type);

        if (self && self.$tip && self.$tip.is(':visible')) {
          self.hoverState = 'in';
          return;
        }

        if (!self) {
          self = new this.constructor(obj.currentTarget, this.getDelegateOptions());
          $(obj.currentTarget).data('bs.' + this.type, self);
        }

        clearTimeout(self.timeout);

        self.hoverState = 'in';

        if (!self.options.delay || !self.options.delay.show) return self.show();

        self.timeout = setTimeout(function () {
          if (self.hoverState == 'in') self.show();
        }, self.options.delay.show);
      };

      Tooltip.prototype.leave = function (obj) {
        var self = obj instanceof this.constructor ?
          obj : $(obj.currentTarget).data('bs.' + this.type);

        if (!self) {
          self = new this.constructor(obj.currentTarget, this.getDelegateOptions());
          $(obj.currentTarget).data('bs.' + this.type, self);
        }

        clearTimeout(self.timeout);

        self.hoverState = 'out';

        if (!self.options.delay || !self.options.delay.hide) return self.hide();

        self.timeout = setTimeout(function () {
          if (self.hoverState == 'out') self.hide();
        }, self.options.delay.hide);
      };

      Tooltip.prototype.show = function () {
        var e = $.Event('show.bs.' + this.type);

        if (this.hasContent() && this.enabled) {
          this.$element.trigger(e);

          var inDom = $.contains(this.$element[0].ownerDocument.documentElement, this.$element[0]);
          if (e.isDefaultPrevented() || !inDom) return;
          var that = this;

          var $tip = this.tip();

          var tipId = this.getUID(this.type);

          this.setContent();
          $tip.attr('id', tipId);
          this.$element.attr('aria-describedby', tipId);

          if (this.options.animation) $tip.addClass('fade');

          var placement = typeof this.options.placement == 'function' ?
            this.options.placement.call(this, $tip[0], this.$element[0]) :
            this.options.placement;

          var autoToken = /\s?auto?\s?/i;
          var autoPlace = autoToken.test(placement);
          if (autoPlace) placement = placement.replace(autoToken, '') || 'top';

          $tip
            .detach()
            .css({ top: 0, left: 0, display: 'block' })
            .addClass(placement)
            .data('bs.' + this.type, this);

          this.options.container ? $tip.appendTo(this.options.container) : $tip.insertAfter(this.$element);

          var pos          = this.getPosition();
          var actualWidth  = $tip[0].offsetWidth;
          var actualHeight = $tip[0].offsetHeight;

          if (autoPlace) {
            var orgPlacement = placement;
            var $container   = this.options.container ? $(this.options.container) : this.$element.parent();
            var containerDim = this.getPosition($container);

            placement = placement == 'bottom' && pos.bottom + actualHeight > containerDim.bottom ? 'top'    :
                        placement == 'top'    && pos.top    - actualHeight < containerDim.top    ? 'bottom' :
                        placement == 'right'  && pos.right  + actualWidth  > containerDim.width  ? 'left'   :
                        placement == 'left'   && pos.left   - actualWidth  < containerDim.left   ? 'right'  :
                        placement;

            $tip
              .removeClass(orgPlacement)
              .addClass(placement);
          }

          var calculatedOffset = this.getCalculatedOffset(placement, pos, actualWidth, actualHeight);

          this.applyPlacement(calculatedOffset, placement);

          var complete = function () {
            var prevHoverState = that.hoverState;
            that.$element.trigger('shown.bs.' + that.type);
            that.hoverState = null;

            if (prevHoverState == 'out') that.leave(that);
          };

          $.support.transition && this.$tip.hasClass('fade') ?
            $tip
              .one('bsTransitionEnd', complete)
              .emulateTransitionEnd(Tooltip.TRANSITION_DURATION) :
            complete();
        }
      };

      Tooltip.prototype.applyPlacement = function (offset, placement) {
        var $tip   = this.tip();
        var width  = $tip[0].offsetWidth;
        var height = $tip[0].offsetHeight;

        // manually read margins because getBoundingClientRect includes difference
        var marginTop = parseInt($tip.css('margin-top'), 10);
        var marginLeft = parseInt($tip.css('margin-left'), 10);

        // we must check for NaN for ie 8/9
        if (isNaN(marginTop))  marginTop  = 0;
        if (isNaN(marginLeft)) marginLeft = 0;

        offset.top  = offset.top  + marginTop;
        offset.left = offset.left + marginLeft;

        // $.fn.offset doesn't round pixel values
        // so we use setOffset directly with our own function B-0
        $.offset.setOffset($tip[0], $.extend({
          using: function (props) {
            $tip.css({
              top: Math.round(props.top),
              left: Math.round(props.left)
            });
          }
        }, offset), 0);

        $tip.addClass('in');

        // check to see if placing tip in new offset caused the tip to resize itself
        var actualWidth  = $tip[0].offsetWidth;
        var actualHeight = $tip[0].offsetHeight;

        if (placement == 'top' && actualHeight != height) {
          offset.top = offset.top + height - actualHeight;
        }

        var delta = this.getViewportAdjustedDelta(placement, offset, actualWidth, actualHeight);

        if (delta.left) offset.left += delta.left;
        else offset.top += delta.top;

        var isVertical          = /top|bottom/.test(placement);
        var arrowDelta          = isVertical ? delta.left * 2 - width + actualWidth : delta.top * 2 - height + actualHeight;
        var arrowOffsetPosition = isVertical ? 'offsetWidth' : 'offsetHeight';

        $tip.offset(offset);
        this.replaceArrow(arrowDelta, $tip[0][arrowOffsetPosition], isVertical);
      };

      Tooltip.prototype.replaceArrow = function (delta, dimension, isHorizontal) {
        this.arrow()
          .css(isHorizontal ? 'left' : 'top', 50 * (1 - delta / dimension) + '%')
          .css(isHorizontal ? 'top' : 'left', '');
      };

      Tooltip.prototype.setContent = function () {
        var $tip  = this.tip();
        var title = this.getTitle();

        $tip.find('.tooltip-inner')[this.options.html ? 'html' : 'text'](title);
        $tip.removeClass('fade in top bottom left right');
      };

      Tooltip.prototype.hide = function (callback) {
        var that = this;
        var $tip = this.tip();
        var e    = $.Event('hide.bs.' + this.type);

        function complete() {
          if (that.hoverState != 'in') $tip.detach();
          that.$element
            .removeAttr('aria-describedby')
            .trigger('hidden.bs.' + that.type);
          callback && callback();
        }

        this.$element.trigger(e);

        if (e.isDefaultPrevented()) return;

        $tip.removeClass('in');

        $.support.transition && this.$tip.hasClass('fade') ?
          $tip
            .one('bsTransitionEnd', complete)
            .emulateTransitionEnd(Tooltip.TRANSITION_DURATION) :
          complete();
/* ===========================================================
 * bootstrap-tooltip.js v2.3.2
 * http://getbootstrap.com/2.3.2/javascript.html#tooltips
 * Inspired by the original jQuery.tipsy by Jason Frame
 * ===========================================================
 * Copyright 2013 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ========================================================== */


!function ($) {

  "use strict"; // jshint ;_;


 /* TOOLTIP PUBLIC CLASS DEFINITION
  * =============================== */

  var Tooltip = function (element, options) {
    this.init('tooltip', element, options)
  }

  Tooltip.prototype = {

    constructor: Tooltip

  , init: function (type, element, options) {
      var eventIn
        , eventOut
        , triggers
        , trigger
        , i

      this.type = type
      this.$element = $(element)
      this.options = this.getOptions(options)
      this.enabled = true

      triggers = this.options.trigger.split(' ')

      for (i = triggers.length; i--;) {
        trigger = triggers[i]
        if (trigger == 'click') {
          this.$element.on('click.' + this.type, this.options.selector, $.proxy(this.toggle, this))
        } else if (trigger != 'manual') {
          eventIn = trigger == 'hover' ? 'mouseenter' : 'focus'
          eventOut = trigger == 'hover' ? 'mouseleave' : 'blur'
          this.$element.on(eventIn + '.' + this.type, this.options.selector, $.proxy(this.enter, this))
          this.$element.on(eventOut + '.' + this.type, this.options.selector, $.proxy(this.leave, this))
        }
      }

      this.options.selector ?
        (this._options = $.extend({}, this.options, { trigger: 'manual', selector: '' })) :
        this.fixTitle()
    }

  , getOptions: function (options) {
      options = $.extend({}, $.fn[this.type].defaults, this.$element.data(), options)

      if (options.delay && typeof options.delay == 'number') {
        options.delay = {
          show: options.delay
        , hide: options.delay
        }
      }

      return options
    }

  , enter: function (e) {
      var defaults = $.fn[this.type].defaults
        , options = {}
        , self

      this._options && $.each(this._options, function (key, value) {
        if (defaults[key] != value) options[key] = value
      }, this)

      self = $(e.currentTarget)[this.type](options).data(this.type)

      if (!self.options.delay || !self.options.delay.show) return self.show()

      clearTimeout(this.timeout)
      self.hoverState = 'in'
      this.timeout = setTimeout(function() {
        if (self.hoverState == 'in') self.show()
      }, self.options.delay.show)
    }

  , leave: function (e) {
      var self = $(e.currentTarget)[this.type](this._options).data(this.type)

      if (this.timeout) clearTimeout(this.timeout)
      if (!self.options.delay || !self.options.delay.hide) return self.hide()

      self.hoverState = 'out'
      this.timeout = setTimeout(function() {
        if (self.hoverState == 'out') self.hide()
      }, self.options.delay.hide)
    }

  , show: function () {
      var $tip
        , pos
        , actualWidth
        , actualHeight
        , placement
        , tp
        , e = $.Event('show')

      if (this.hasContent() && this.enabled) {
        this.$element.trigger(e)
        if (e.isDefaultPrevented()) return
        $tip = this.tip()
        this.setContent()

        if (this.options.animation) {
          $tip.addClass('fade')
        }

        placement = typeof this.options.placement == 'function' ?
          this.options.placement.call(this, $tip[0], this.$element[0]) :
          this.options.placement

        $tip
          .detach()
          .css({ top: 0, left: 0, display: 'block' })

        this.options.container ? $tip.appendTo(this.options.container) : $tip.insertAfter(this.$element)

        pos = this.getPosition()

        actualWidth = $tip[0].offsetWidth
        actualHeight = $tip[0].offsetHeight

        switch (placement) {
          case 'bottom':
            tp = {top: pos.top + pos.height, left: pos.left + pos.width / 2 - actualWidth / 2}
            break
          case 'top':
            tp = {top: pos.top - actualHeight, left: pos.left + pos.width / 2 - actualWidth / 2}
            break
          case 'left':
            tp = {top: pos.top + pos.height / 2 - actualHeight / 2, left: pos.left - actualWidth}
            break
          case 'right':
            tp = {top: pos.top + pos.height / 2 - actualHeight / 2, left: pos.left + pos.width}
            break
        }

        this.applyPlacement(tp, placement)
        this.$element.trigger('shown')
      }
    }

  , applyPlacement: function(offset, placement){
      var $tip = this.tip()
        , width = $tip[0].offsetWidth
        , height = $tip[0].offsetHeight
        , actualWidth
        , actualHeight
        , delta
        , replace

      $tip
        .offset(offset)
        .addClass(placement)
        .addClass('in')

      actualWidth = $tip[0].offsetWidth
      actualHeight = $tip[0].offsetHeight

      if (placement == 'top' && actualHeight != height) {
        offset.top = offset.top + height - actualHeight
        replace = true
      }

      if (placement == 'bottom' || placement == 'top') {
        delta = 0

        if (offset.left < 0){
          delta = offset.left * -2
          offset.left = 0
          $tip.offset(offset)
          actualWidth = $tip[0].offsetWidth
          actualHeight = $tip[0].offsetHeight
        }

        this.replaceArrow(delta - width + actualWidth, actualWidth, 'left')
      } else {
        this.replaceArrow(actualHeight - height, actualHeight, 'top')
      }

      if (replace) $tip.offset(offset)
    }

  , replaceArrow: function(delta, dimension, position){
      this
        .arrow()
        .css(position, delta ? (50 * (1 - delta / dimension) + "%") : '')
    }

  , setContent: function () {
      var $tip = this.tip()
        , title = this.getTitle()

      $tip.find('.tooltip-inner')[this.options.html ? 'html' : 'text'](title)
      $tip.removeClass('fade in top bottom left right')
    }

  , hide: function () {
      var that = this
        , $tip = this.tip()
        , e = $.Event('hide')

      this.$element.trigger(e)
      if (e.isDefaultPrevented()) return

      $tip.removeClass('in')

      function removeWithAnimation() {
        var timeout = setTimeout(function () {
          $tip.off($.support.transition.end).detach()
        }, 500)

        $tip.one($.support.transition.end, function () {
          clearTimeout(timeout)
          $tip.detach()
        })
      }

      $.support.transition && this.$tip.hasClass('fade') ?
        removeWithAnimation() :
        $tip.detach()

      this.$element.trigger('hidden')

      return this
    }

  , fixTitle: function () {
      var $e = this.$element
      if ($e.attr('title') || typeof($e.attr('data-original-title')) != 'string') {
        $e.attr('data-original-title', $e.attr('title') || '').attr('title', '')
      }
    }

  , hasContent: function () {
      return this.getTitle()
    }

  , getPosition: function () {
      var el = this.$element[0]
      return $.extend({}, (typeof el.getBoundingClientRect == 'function') ? el.getBoundingClientRect() : {
        width: el.offsetWidth
      , height: el.offsetHeight
      }, this.$element.offset())
    }

  , getTitle: function () {
      var title
        , $e = this.$element
        , o = this.options

      title = $e.attr('data-original-title')
        || (typeof o.title == 'function' ? o.title.call($e[0]) :  o.title)

      return title
    }

  , tip: function () {
      return this.$tip = this.$tip || $(this.options.template)
    }

  , arrow: function(){
      return this.$arrow = this.$arrow || this.tip().find(".tooltip-arrow")
    }

  , validate: function () {
      if (!this.$element[0].parentNode) {
        this.hide()
        this.$element = null
        this.options = null
      }
    }

  , enable: function () {
      this.enabled = true
    }

  , disable: function () {
      this.enabled = false
    }

  , toggleEnabled: function () {
      this.enabled = !this.enabled
    }

  , toggle: function (e) {
      var self = e ? $(e.currentTarget)[this.type](this._options).data(this.type) : this
      self.tip().hasClass('in') ? self.hide() : self.show()
    }

  , destroy: function () {
      this.hide().$element.off('.' + this.type).removeData(this.type)
    }

  }


 /* TOOLTIP PLUGIN DEFINITION
  * ========================= */

  var old = $.fn.tooltip

  $.fn.tooltip = function ( option ) {
    return this.each(function () {
      var $this = $(this)
        , data = $this.data('tooltip')
        , options = typeof option == 'object' && option
      if (!data) $this.data('tooltip', (data = new Tooltip(this, options)))
      if (typeof option == 'string') data[option]()
    })
  }

  $.fn.tooltip.Constructor = Tooltip

  $.fn.tooltip.defaults = {
    animation: true
  , placement: 'top'
  , selector: false
  , template: '<div class="tooltip"><div class="tooltip-arrow"></div><div class="tooltip-inner"></div></div>'
  , trigger: 'hover focus'
  , title: ''
  , delay: 0
  , html: false
  , container: false
  }


 /* TOOLTIP NO CONFLICT
  * =================== */

  $.fn.tooltip.noConflict = function () {
    $.fn.tooltip = old
    return this
  }

}(window.jQuery);

        this.hoverState = null;

        return this;
      };

      Tooltip.prototype.fixTitle = function () {
        var $e = this.$element;
        if ($e.attr('title') || typeof ($e.attr('data-original-title')) != 'string') {
          $e.attr('data-original-title', $e.attr('title') || '').attr('title', '');
        }
      };

      Tooltip.prototype.hasContent = function () {
        return this.getTitle();
      };

      Tooltip.prototype.getPosition = function ($element) {
        $element   = $element || this.$element;

        var el     = $element[0];
        var isBody = el.tagName == 'BODY';

        var elRect    = el.getBoundingClientRect();
        if (elRect.width == null) {
          // width and height are missing in IE8, so compute them manually; see https://github.com/twbs/bootstrap/issues/14093
          elRect = $.extend({}, elRect, { width: elRect.right - elRect.left, height: elRect.bottom - elRect.top });
        }
        var elOffset  = isBody ? { top: 0, left: 0 } : $element.offset();
        var scroll    = { scroll: isBody ? document.documentElement.scrollTop || document.body.scrollTop : $element.scrollTop() };
        var outerDims = isBody ? { width: $(window).width(), height: $(window).height() } : null;

        return $.extend({}, elRect, scroll, outerDims, elOffset);
      };

      Tooltip.prototype.getCalculatedOffset = function (placement, pos, actualWidth, actualHeight) {
        return placement == 'bottom' ? { top: pos.top + pos.height,   left: pos.left + pos.width / 2 - actualWidth / 2  } :
               placement == 'top'    ? { top: pos.top - actualHeight, left: pos.left + pos.width / 2 - actualWidth / 2  } :
               placement == 'left'   ? { top: pos.top + pos.height / 2 - actualHeight / 2, left: pos.left - actualWidth } :
            /* placement == 'right' */ { top: pos.top + pos.height / 2 - actualHeight / 2, left: pos.left + pos.width   };

      };

      Tooltip.prototype.getViewportAdjustedDelta = function (placement, pos, actualWidth, actualHeight) {
        var delta = { top: 0, left: 0 };
        if (!this.$viewport) return delta;

        var viewportPadding = this.options.viewport && this.options.viewport.padding || 0;
        var viewportDimensions = this.getPosition(this.$viewport);

        if (/right|left/.test(placement)) {
          var topEdgeOffset    = pos.top - viewportPadding - viewportDimensions.scroll;
          var bottomEdgeOffset = pos.top + viewportPadding - viewportDimensions.scroll + actualHeight;
          if (topEdgeOffset < viewportDimensions.top) { // top overflow
            delta.top = viewportDimensions.top - topEdgeOffset;
          } else if (bottomEdgeOffset > viewportDimensions.top + viewportDimensions.height) { // bottom overflow
            delta.top = viewportDimensions.top + viewportDimensions.height - bottomEdgeOffset;
          }
        } else {
          var leftEdgeOffset  = pos.left - viewportPadding;
          var rightEdgeOffset = pos.left + viewportPadding + actualWidth;
          if (leftEdgeOffset < viewportDimensions.left) { // left overflow
            delta.left = viewportDimensions.left - leftEdgeOffset;
          } else if (rightEdgeOffset > viewportDimensions.width) { // right overflow
            delta.left = viewportDimensions.left + viewportDimensions.width - rightEdgeOffset;
          }
        }

        return delta;
      };

      Tooltip.prototype.getTitle = function () {
        var title;
        var $e = this.$element;
        var o  = this.options;

        title = $e.attr('data-original-title')
          || (typeof o.title == 'function' ? o.title.call($e[0]) :  o.title);

        return title;
      };

      Tooltip.prototype.getUID = function (prefix) {
        do prefix += ~~(Math.random() * 1000000);
        while (document.getElementById(prefix));
        return prefix;
      };

      Tooltip.prototype.tip = function () {
        return (this.$tip = this.$tip || $(this.options.template));
      };

      Tooltip.prototype.arrow = function () {
        return (this.$arrow = this.$arrow || this.tip().find('.tooltip-arrow'));
      };

      Tooltip.prototype.enable = function () {
        this.enabled = true;
      };

      Tooltip.prototype.disable = function () {
        this.enabled = false;
      };

      Tooltip.prototype.toggleEnabled = function () {
        this.enabled = !this.enabled;
      };

      Tooltip.prototype.toggle = function (e) {
        var self = this;
        if (e) {
          self = $(e.currentTarget).data('bs.' + this.type);
          if (!self) {
            self = new this.constructor(e.currentTarget, this.getDelegateOptions());
            $(e.currentTarget).data('bs.' + this.type, self);
          }
        }

        self.tip().hasClass('in') ? self.leave(self) : self.enter(self);
      };

      Tooltip.prototype.destroy = function () {
        var that = this;
        clearTimeout(this.timeout);
        this.hide(function () {
          that.$element.off('.' + that.type).removeData('bs.' + that.type);
        });
      };


      // TOOLTIP PLUGIN DEFINITION
      // =========================

      function Plugin(option) {
        return this.each(function () {
          var $this    = $(this);
          var data     = $this.data('bs.tooltip');
          var options  = typeof option == 'object' && option;
          var selector = options && options.selector;

          if (!data && option == 'destroy') return;
          if (selector) {
            if (!data) $this.data('bs.tooltip', (data = {}));
            if (!data[selector]) data[selector] = new Tooltip(this, options);
          } else {
            if (!data) $this.data('bs.tooltip', (data = new Tooltip(this, options)));
          }
          if (typeof option == 'string') data[option]();
        });
      }

      var old = $.fn.tooltip;

      $.fn.tooltip             = Plugin;
      $.fn.tooltip.Constructor = Tooltip;


      // TOOLTIP NO CONFLICT
      // ===================

      $.fn.tooltip.noConflict = function () {
        $.fn.tooltip = old;
        return this;
      };

    }(jQuery);

    /* ========================================================================
     * Bootstrap: popover.js v3.3.1
     * http://getbootstrap.com/javascript/#popovers
     * ========================================================================
     * Copyright 2011-2014 Twitter, Inc.
     * Licensed under MIT (https://github.com/twbs/bootstrap/blob/master/LICENSE)
     * ======================================================================== */


    +function ($) {
      'use strict';

      if(typeof $().popover == 'function')
        return true;

      // POPOVER PUBLIC CLASS DEFINITION
      // ===============================

      var Popover = function (element, options) {
        this.init('popover', element, options);
      };

      if (!$.fn.tooltip) throw new Error('Popover requires tooltip.js');

      Popover.VERSION  = '3.3.1';

      Popover.DEFAULTS = $.extend({}, $.fn.tooltip.Constructor.DEFAULTS, {
        placement: 'right',
        trigger: 'click',
        content: '',
        template: '<div class="popover" role="tooltip"><div class="arrow"></div><h3 class="popover-title"></h3><div class="popover-content"></div></div>'
      });


      // NOTE: POPOVER EXTENDS tooltip.js
      // ================================

      Popover.prototype = $.extend({}, $.fn.tooltip.Constructor.prototype);

      Popover.prototype.constructor = Popover;

      Popover.prototype.getDefaults = function () {
        return Popover.DEFAULTS;
      };

      Popover.prototype.setContent = function () {
        var $tip    = this.tip();
        var title   = this.getTitle();
        var content = this.getContent();

        $tip.find('.popover-title')[this.options.html ? 'html' : 'text'](title);
        $tip.find('.popover-content').children().detach().end()[ // we use append for html objects to maintain js events
          this.options.html ? (typeof content == 'string' ? 'html' : 'append') : 'text'
        ](content);

        $tip.removeClass('fade top bottom left right in');

        // IE8 doesn't accept hiding via the `:empty` pseudo selector, we have to do
        // this manually by checking the contents.
        if (!$tip.find('.popover-title').html()) $tip.find('.popover-title').hide();
      };

      Popover.prototype.hasContent = function () {
        return this.getTitle() || this.getContent();
      };

      Popover.prototype.getContent = function () {
        var $e = this.$element;
        var o  = this.options;

        return $e.attr('data-content')
          || (typeof o.content == 'function' ?
                o.content.call($e[0]) :
                o.content);
      };

      Popover.prototype.arrow = function () {
        return (this.$arrow = this.$arrow || this.tip().find('.arrow'));
      };

      Popover.prototype.tip = function () {
        if (!this.$tip) this.$tip = $(this.options.template);
        return this.$tip;
      };


      // POPOVER PLUGIN DEFINITION
      // =========================

      function Plugin(option) {
        return this.each(function () {
          var $this    = $(this);
          var data     = $this.data('bs.popover');
          var options  = typeof option == 'object' && option;
          var selector = options && options.selector;

          if (!data && option == 'destroy') return;
          if (selector) {
            if (!data) $this.data('bs.popover', (data = {}));
            if (!data[selector]) data[selector] = new Popover(this, options);
          } else {
            if (!data) $this.data('bs.popover', (data = new Popover(this, options)));
          }
          if (typeof option == 'string') data[option]();
        });
      }

      var old = $.fn.popover;

      $.fn.popover             = Plugin;
      $.fn.popover.Constructor = Popover;


      // POPOVER NO CONFLICT
      // ===================

      $.fn.popover.noConflict = function () {
        $.fn.popover = old;
        return this;
      };

    }(jQuery);
}