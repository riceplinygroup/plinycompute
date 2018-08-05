angular
.module('app')
.directive('includeReplace', includeReplace)
.directive('a', preventClickDirective)
.directive('a', bootstrapCollapseDirective)
.directive('a', navigationDirective)
.directive('button', layoutToggleDirective)
.directive('a', layoutToggleDirective)
.directive('button', collapseMenuTogglerDirective)
.directive('div', bootstrapCarouselDirective)
.directive('toggle', bootstrapTooltipsPopoversDirective)
.directive('tab', bootstrapTabsDirective)
.directive('button', cardCollapseDirective)

function includeReplace() {
  return {
      require: 'ngInclude',
      restrict: 'A',
      link: link
  };

  function link(scope, element, attrs) {
    element.replaceWith(element.children());
  }
}

//Prevent click if href="#"
function preventClickDirective() {
  return {
      restrict: 'E',
      link: link
  };

  function link(scope, element, attrs) {
    if (attrs.href === '#'){
      element.on('click', function(event){
        event.preventDefault();
      });
    }
  }
}

//Bootstrap Collapse
function bootstrapCollapseDirective() {
  return {
      restrict: 'E',
      link: link
  };

  function link(scope, element, attrs) {
    if (attrs.toggle=='collapse'){
      element.attr('href','javascript;;').attr('data-target',attrs.href.replace('index.html',''));
    }
  }
}

/**
* @desc Genesis main navigation - Siedebar menu
* @example <li class="nav-item nav-dropdown"></li>
*/
function navigationDirective() {
  return {
      restrict: 'E',
      link: link
  };

  function link(scope, element, attrs) {
    if(element.hasClass('nav-dropdown-toggle') && angular.element('body').width() > 782) {
      element.on('click', function(){
        if(!angular.element('body').hasClass('compact-nav')) {
          element.parent().toggleClass('open').find('.open').removeClass('open');
        }
      });
    } else if (element.hasClass('nav-dropdown-toggle') && angular.element('body').width() < 783) {
      element.on('click', function(){
        element.parent().toggleClass('open').find('.open').removeClass('open');
      });
    }
  }
}

//Dynamic resize .sidebar-nav
sidebarNavDynamicResizeDirective.$inject = ['$window', '$timeout'];
function sidebarNavDynamicResizeDirective($window, $timeout) {
  return {
      restrict: 'E',
      link: link
  };

  function link(scope, element, attrs) {

    if (element.hasClass('sidebar-nav') && angular.element('body').hasClass('fixed-nav')) {
      var bodyHeight = angular.element(window).height();
      scope.$watch(function(){
        var headerHeight = angular.element('header').outerHeight();

        if (angular.element('body').hasClass('sidebar-off-canvas')) {
          element.css('height', bodyHeight);
        } else {
          element.css('height', bodyHeight - headerHeight);
        }
      });

      angular.element($window).bind('resize', function(){
        var bodyHeight = angular.element(window).height();
        var headerHeight = angular.element('header').outerHeight();
        var sidebarHeaderHeight = angular.element('.sidebar-header').outerHeight();
        var sidebarFooterHeight = angular.element('.sidebar-footer').outerHeight();

        if (angular.element('body').hasClass('sidebar-off-canvas')) {
          element.css('height', bodyHeight - sidebarHeaderHeight - sidebarFooterHeight);
        } else {
          element.css('height', bodyHeight - headerHeight - sidebarHeaderHeight - sidebarFooterHeight);
        }
      });
    }
  }
}

//LayoutToggle
layoutToggleDirective.$inject = ['$interval'];
function layoutToggleDirective($interval) {
  return {
      restrict: 'E',
      link: link
  };

  function link(scope, element, attrs) {
    element.on('click', function(){

      if (element.hasClass('sidebar-toggler')) {
        angular.element('body').toggleClass('sidebar-hidden');
      }

      if (element.hasClass('aside-menu-toggler')) {
        angular.element('body').toggleClass('aside-menu-hidden');
      }
    });
  }
}

//collapse menu toggler
function collapseMenuTogglerDirective() {
  return {
      restrict: 'E',
      link: link
  };

  function link(scope, element, attrs) {
    element.on('click', function(){
      if (element.hasClass('navbar-toggler') && !element.hasClass('layout-toggler')) {
        angular.element('body').toggleClass('sidebar-mobile-show')
      }
    })
  }
}

//bootstrap carousel
function bootstrapCarouselDirective() {
  return {
      restrict: 'E',
      link: link
  };

  function link(scope, element, attrs) {
    if (attrs.ride === 'carousel'){
      element.find('a').each(function(){
        $(this).attr('data-target',$(this).attr('href').replace('index.html','')).attr('href','javascript;;')
      });
    }
  }
}

//bootstrap tooltips & popovers
function bootstrapTooltipsPopoversDirective() {

  return {
      restrict: 'A',
      link: link
  };

  function link(scope, element, attrs) {
    if (attrs.toggle === 'tooltip'){
      angular.element(element).tooltip();
    }
    if (attrs.toggle === 'popover'){
      angular.element(element).popover();
    }
  }
}

//Bootstrap Tabs
function bootstrapTabsDirective() {

  return {
      restrict: 'A',
      link: link
  };

  function link(scope, element, attrs) {
    element.click(function(e) {
      e.preventDefault();
      angular.element(element).tab('show');
    });
  }
}

// card collapse
function cardCollapseDirective() {

  return {
      restrict: 'E',
      link: link
  };

  function link(scope, element, attrs) {
    if (attrs.toggle === 'collapse' && element.parent().hasClass('card-actions')){

      if (element.parent().parent().parent().find('.card-body').hasClass('in')) {
        element.find('i').addClass('r180');
      }

      var id = 'collapse-' + Math.floor((Math.random() * 1000000000) + 1);
      element.attr('data-target','#'+id)
      element.parent().parent().parent().find('.card-body').attr('id',id);

      element.on('click', function(){
        element.find('i').toggleClass('r180');
      })
    }
  }
}
