// Similar trait to Borrow, except can return a struct with a lifetime.
pub trait AsView<'v, View: Sized + 'v> {
    fn as_view(&'v self) -> View;
}

pub trait ToOwnedView<'v>: Sized
where
    Self: 'v,
{
    type Owned: AsView<'v, Self>;

    fn to_owned_view(&'v self) -> Self::Owned;
}

impl<'v, View: 'v, Owned: AsView<'v, View>> AsView<'v, Option<View>> for Option<Owned> {
    fn as_view(&'v self) -> Option<View> {
        match self {
            None => None,
            Some(v) => Some(v.as_view()),
        }
    }
}

impl<'v, Owned, View> ToOwnedView<'v> for Option<View>
where
    View: ToOwnedView<'v, Owned = Owned> + 'v,
    Owned: AsView<'v, View>,
{
    type Owned = Option<Owned>;

    fn to_owned_view(&'v self) -> Self::Owned {
        match self {
            None => None,
            Some(view) => Some(view.to_owned_view()),
        }
    }
}
